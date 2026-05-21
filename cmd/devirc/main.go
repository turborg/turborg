// Command devirc is a minimal IRC stub for local dev. It speaks just
// enough IRC (CAP / NICK / USER / AUTHENTICATE / JOIN / PING) for a
// turborg agent to complete its handshake and look "registered", then
// exposes an HTTP control plane on a separate port so you can trigger
// netsplit-shaped failures from the shell.
//
// Listens plain TCP only — disable TLS in the bot's connector config
// (TURBORG_IRC_USE_TLS=false) when pointing it here.
//
// HTTP control endpoints (POST):
//
//	/kill    close the active client connection (simulate a netsplit
//	         drop — bot's read loop sees EOF, supervisor reconnects)
//	/pause   /kill, plus stop accepting new connections until /resume
//	         (simulate a fully unreachable upstream — bot churns through
//	         the backoff schedule)
//	/resume  re-open the listener
//	/status  return current state (clients, totalAccepted, paused)
//
// Example:
//
//	go run ./cmd/devirc -irc :16667 -ctrl :16668
//	# wait until the bot connects (see status)
//	curl -X POST localhost:16668/kill
//	# bot reconnects on its own — kill again to bounce, /pause to stall
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	ircAddr := flag.String("irc", ":16667", "IRC listen address (plain TCP, no TLS)")
	ctrlAddr := flag.String("ctrl", ":16668", "HTTP control-plane listen address")
	serverName := flag.String("server", "devirc", "Server name used in server-originated lines")
	flag.Parse()

	s := newServer(*serverName)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go s.runIRC(ctx, *ircAddr)
	go s.runCtrl(ctx, *ctrlAddr)

	log.Printf("devirc: IRC=%s ctrl=%s server=%s", *ircAddr, *ctrlAddr, *serverName)
	log.Printf("devirc: try `curl -X POST localhost%s/kill` to simulate a netsplit", *ctrlAddr)
	<-ctx.Done()
	log.Printf("devirc: shutting down")
	s.shutdown()
}

type server struct {
	serverName string

	mu             sync.Mutex
	listener       net.Listener
	clients        map[*client]struct{}
	paused         bool
	totalAccepted  int64
	totalDropped   int64
	totalHandshake int64
}

type client struct {
	conn  net.Conn
	nick  string
	w     *bufio.Writer
	wmu   sync.Mutex
	saslState string // "", "PLAIN", "done"
	capLS bool
	registered bool
}

func newServer(name string) *server {
	return &server{
		serverName: name,
		clients:    make(map[*client]struct{}),
	}
}

func (s *server) runIRC(ctx context.Context, addr string) {
	for {
		if err := s.listenIRC(ctx, addr); err != nil && ctx.Err() == nil {
			log.Printf("devirc IRC listener: %v — retrying in 1s", err)
			time.Sleep(1 * time.Second)
			continue
		}
		return
	}
}

func (s *server) listenIRC(ctx context.Context, addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = l
	s.mu.Unlock()
	go func() { <-ctx.Done(); _ = l.Close() }()

	for {
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Paused/closed listener will surface here as ErrClosed; outer
			// loop sleeps + retries until /resume re-opens.
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			log.Printf("devirc accept: %v", err)
			continue
		}
		s.mu.Lock()
		s.totalAccepted++
		s.mu.Unlock()
		c := &client{conn: conn, w: bufio.NewWriter(conn)}
		s.mu.Lock()
		s.clients[c] = struct{}{}
		s.mu.Unlock()
		log.Printf("devirc: client connected %s", conn.RemoteAddr())
		go s.serveClient(c)
	}
}

func (s *server) serveClient(c *client) {
	defer func() {
		_ = c.conn.Close()
		s.mu.Lock()
		delete(s.clients, c)
		s.mu.Unlock()
		log.Printf("devirc: client gone (nick=%q registered=%v)", c.nick, c.registered)
	}()
	r := bufio.NewReader(c.conn)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			continue
		}
		s.handle(c, line)
	}
}

// handle dispatches one IRC line. The branch count is naturally high
// (one case per supported command); splitting into per-command methods
// would be more noise than signal for a 200-line stub.
//
//nolint:gocyclo // intentional: dispatch table for a dev IRC stub
func (s *server) handle(c *client, line string) {
	upper := strings.ToUpper(line)
	switch {
	case strings.HasPrefix(upper, "CAP LS"):
		c.send(":" + s.serverName + " CAP * LS :sasl message-tags server-time multi-prefix")
		c.capLS = true
	case strings.HasPrefix(upper, "CAP REQ"):
		idx := strings.Index(line, ":")
		caps := ""
		if idx >= 0 && idx+1 < len(line) {
			caps = strings.TrimSpace(line[idx+1:])
		}
		c.send(":" + s.serverName + " CAP * ACK :" + caps)
	case upper == "CAP END":
		// no-op; client signals end of CAP negotiation
	case strings.HasPrefix(upper, "AUTHENTICATE PLAIN"):
		c.saslState = "PLAIN"
		c.send("AUTHENTICATE +")
	case strings.HasPrefix(upper, "AUTHENTICATE "):
		if c.saslState == "PLAIN" {
			c.send(":" + s.serverName + " 903 " + nz(c.nick, "*") + " :SASL successful")
			c.saslState = "done"
		}
	case strings.HasPrefix(upper, "NICK "):
		c.nick = strings.TrimSpace(line[5:])
	case strings.HasPrefix(upper, "USER "):
		if c.nick == "" {
			c.nick = "guest"
		}
		nick := c.nick
		c.send(":" + s.serverName + " 001 " + nick + " :Welcome to devirc")
		c.send(":" + s.serverName + " 002 " + nick + " :Your host is " + s.serverName)
		c.send(":" + s.serverName + " 003 " + nick + " :This server was created today")
		c.send(":" + s.serverName + " 004 " + nick + " " + s.serverName + " devirc-1 o o")
		c.send(":" + s.serverName + " 005 " + nick + " PREFIX=(ohv)@%+ CHANTYPES=# :are supported by this server")
		c.send(":" + s.serverName + " 375 " + nick + " :- devirc MOTD -")
		c.send(":" + s.serverName + " 372 " + nick + " :- this is a dev IRC stub")
		c.send(":" + s.serverName + " 376 " + nick + " :End of MOTD command")
		c.registered = true
		s.mu.Lock()
		s.totalHandshake++
		s.mu.Unlock()
	case strings.HasPrefix(upper, "PING "):
		token := strings.TrimSpace(line[5:])
		token = strings.TrimPrefix(token, ":")
		c.send(":" + s.serverName + " PONG " + s.serverName + " :" + token)
	case strings.HasPrefix(upper, "JOIN "):
		ch := strings.TrimSpace(line[5:])
		// Channel can be "#foo" or "#foo key"
		if sp := strings.IndexByte(ch, ' '); sp >= 0 {
			ch = ch[:sp]
		}
		nick := nz(c.nick, "*")
		c.send(":" + nick + "!~u@host JOIN " + ch)
		c.send(":" + s.serverName + " 332 " + nick + " " + ch + " :devirc topic")
		c.send(":" + s.serverName + " 353 " + nick + " = " + ch + " :@" + nick)
		c.send(":" + s.serverName + " 366 " + nick + " " + ch + " :End of NAMES list")
	}
	// Lines we don't pattern-match (PRIVMSG, NOTICE, QUIT, ...) silently
	// fall through. QUIT in particular doesn't need a handler — the bot
	// closes its side after sending it; the read loop hits EOF and
	// serveClient unwinds.
}

func (c *client) send(line string) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, _ = c.w.WriteString(line + "\r\n")
	_ = c.w.Flush()
}

// runCtrl serves the HTTP control plane.
func (s *server) runCtrl(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/kill", s.handleKill)
	mux.HandleFunc("/pause", s.handlePause)
	mux.HandleFunc("/resume", s.handleResume)
	mux.HandleFunc("/status", s.handleStatus)

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		// Detach the shutdown deadline from the parent ctx — it's
		// already cancelled, so srv.Shutdown would return immediately
		// with ctx.Err. WithoutCancel + WithTimeout gives the graceful
		// shutdown a real 2s budget independent of the trigger ctx.
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("devirc control: %v", err)
	}
}

func (s *server) handleKill(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	n := s.killAll()
	log.Printf("devirc: /kill dropped %d client(s)", n)
	respondJSON(w, map[string]any{"dropped": n})
}

func (s *server) handlePause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	s.paused = true
	l := s.listener
	s.listener = nil
	s.mu.Unlock()
	if l != nil {
		_ = l.Close()
	}
	n := s.killAll()
	log.Printf("devirc: /pause — listener closed, %d client(s) dropped", n)
	respondJSON(w, map[string]any{"paused": true, "dropped": n})
}

func (s *server) handleResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	wasPaused := s.paused
	s.paused = false
	s.mu.Unlock()
	if wasPaused {
		log.Printf("devirc: /resume — listener will re-open on the next outer-loop tick")
	}
	respondJSON(w, map[string]any{"paused": false, "was_paused": wasPaused})
}

func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	clients := make([]map[string]any, 0, len(s.clients))
	for c := range s.clients {
		clients = append(clients, map[string]any{
			"remote":     c.conn.RemoteAddr().String(),
			"nick":       c.nick,
			"registered": c.registered,
		})
	}
	resp := map[string]any{
		"paused":          s.paused,
		"clients":         clients,
		"total_accepted":  s.totalAccepted,
		"total_handshake": s.totalHandshake,
		"total_dropped":   s.totalDropped,
	}
	s.mu.Unlock()
	respondJSON(w, resp)
}

func (s *server) killAll() int {
	s.mu.Lock()
	conns := make([]net.Conn, 0, len(s.clients))
	for c := range s.clients {
		conns = append(conns, c.conn)
	}
	s.totalDropped += int64(len(conns))
	s.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	return len(conns)
}

func (s *server) shutdown() {
	s.mu.Lock()
	l := s.listener
	s.listener = nil
	s.mu.Unlock()
	if l != nil {
		_ = l.Close()
	}
	s.killAll()
}

func respondJSON(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func nz(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

var _ = fmt.Sprintf

package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

// fakeIRC is a minimal, non-test IRC sink for load generation: it completes
// registration (CAP ACK + RPL_WELCOME), echoes JOINs, answers PINGs, and
// drains everything else. Enough for N synthetic tenant connectors to reach
// "registered" and JOIN so the pooled runtime's steady-state footprint can be
// measured. (The tests/fixtures/fakeirc fixture requires a testing.TB, so it
// can't be reused from a binary.)
type fakeIRC struct {
	ln   net.Listener
	wg   sync.WaitGroup
	live atomic.Int64 // currently-registered connections
}

func startFakeIRC() (*fakeIRC, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	f := &fakeIRC{ln: ln}
	f.wg.Add(1)
	go f.acceptLoop()
	return f, nil
}

func (f *fakeIRC) addr() string    { return f.ln.Addr().String() }
func (f *fakeIRC) registered() int { return int(f.live.Load()) }

func (f *fakeIRC) acceptLoop() {
	defer f.wg.Done()
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return // listener closed
		}
		f.wg.Add(1)
		go f.handle(conn)
	}
}

func (f *fakeIRC) handle(conn net.Conn) {
	defer f.wg.Done()
	defer func() { _ = conn.Close() }()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	send := func(s string) {
		_, _ = fmt.Fprintf(w, "%s\r\n", s)
		_ = w.Flush()
	}

	nick := "user"
	registered := false
	defer func() {
		if registered {
			f.live.Add(-1)
		}
	}()

	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(line, "NICK "):
			nick = strings.TrimSpace(line[len("NICK "):])
		case strings.HasPrefix(line, "CAP LS"):
			send(":fake CAP * LS :")
		case strings.HasPrefix(line, "CAP REQ"):
			if i := strings.Index(line, " :"); i >= 0 {
				send(":fake CAP * ACK :" + line[i+2:])
			}
		case strings.HasPrefix(line, "USER "):
			send(fmt.Sprintf(":fake 001 %s :Welcome", nick))
			send(fmt.Sprintf(":fake 376 %s :End of MOTD", nick))
			if !registered {
				registered = true
				f.live.Add(1)
			}
		case strings.HasPrefix(line, "PING "):
			payload := strings.TrimPrefix(strings.TrimSpace(line[len("PING"):]), ":")
			send(":fake PONG fake :" + payload)
		case strings.HasPrefix(line, "JOIN "):
			target := strings.TrimSpace(line[len("JOIN "):])
			send(fmt.Sprintf(":%s!~u@host JOIN %s", nick, target))
		}
	}
}

func (f *fakeIRC) close() {
	_ = f.ln.Close()
	f.wg.Wait()
}

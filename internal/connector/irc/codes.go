package irc

// IRC numeric replies turborg recognizes during the handshake or runtime.
// String values match Python core/connectors/irc/codes.py byte-for-byte so
// dashboards and logs stay stable across the port.
const (
	RplWelcome       = "001"
	RplYourHost      = "002"
	RplCreated       = "003"
	RplMyInfo        = "004"
	RplWhoisUser     = "311"
	RplWhoisServer   = "312"
	RplWhoisOperator = "313"
	RplEndOfWho      = "315"
	RplWhoisIdle     = "317"
	RplEndOfWhois    = "318"
	RplWhoisChannels = "319"
	RplWhoisAccount  = "330"
	RplListStart     = "321"
	RplList          = "322"
	RplListEnd       = "323"
	RplNoTopic       = "331"
	RplTopic         = "332"
	RplTopicWhoTime  = "333"
	RplWhoReply      = "352"
	RplNamReply      = "353"
	RplEndOfNames    = "366"
	RplEndOfMOTD     = "376"
	RplWhoisSecure   = "671"

	RplSaslLoggedIn  = "900"
	RplSaslSuccess   = "903"
	ErrSaslFail      = "904"
	ErrSaslTooLong   = "905"
	ErrSaslAborted   = "906"
	ErrSaslAlready   = "907"

	ErrNoSuchNick      = "401"
	ErrNoMOTD          = "422"
	ErrNickNameInUse   = "433"
	ErrNotRegistered   = "451"
	ErrPasswdMismatch  = "464"
	ErrChannelIsFull   = "471"
	ErrInviteOnlyChan  = "473"
	ErrBannedFromChan  = "474"
	ErrBadChannelKey   = "475"
)

// IRC commands turborg sends or recognizes on the wire.
const (
	CmdPrivmsg       = "PRIVMSG"
	CmdNotice        = "NOTICE"
	CmdJoin          = "JOIN"
	CmdPart          = "PART"
	CmdQuit          = "QUIT"
	CmdNick          = "NICK"
	CmdUser          = "USER"
	CmdPass          = "PASS"
	CmdPing          = "PING"
	CmdPong          = "PONG"
	CmdMode          = "MODE"
	CmdTopic         = "TOPIC"
	CmdKick          = "KICK"
	CmdNames         = "NAMES"
	CmdCap           = "CAP"
	CmdWhois         = "WHOIS"
	CmdInvite        = "INVITE"
	CmdAway          = "AWAY"
	CmdList          = "LIST"
	CmdWho           = "WHO"
	CmdAuthenticate  = "AUTHENTICATE"
)

// IsHandshakeComplete reports whether the given numeric reply signals the
// server has finished sending its MOTD and the connection is ready for
// normal traffic. Matches Python's HANDSHAKE_COMPLETE frozenset.
func IsHandshakeComplete(numeric string) bool {
	return numeric == RplEndOfMOTD || numeric == ErrNoMOTD
}

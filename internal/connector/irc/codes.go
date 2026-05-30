package irc

// IRC numeric replies turborg recognizes during the handshake or runtime.
// Identifiers follow the conventional Rpl*/Err* naming; the string
// values are the wire-level numerics defined by RFC 1459/2812.
const (
	RplWelcome       = "001"
	RplYourHost      = "002"
	RplCreated       = "003"
	RplMyInfo        = "004"
	RplISupport      = "005"
	RplLUserClient   = "251"
	RplLUserOp       = "252"
	RplLUserUnknown  = "253"
	RplLUserChannels = "254"
	RplLUserMe       = "255"
	RplMOTD          = "372"
	RplMOTDStart     = "375"
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

	RplSaslLoggedIn = "900"
	RplSaslSuccess  = "903"
	ErrSaslFail     = "904"
	ErrSaslTooLong  = "905"
	ErrSaslAborted  = "906"
	ErrSaslAlready  = "907"

	ErrNoSuchNick       = "401"
	ErrNoMOTD           = "422"
	ErrNickNameInUse    = "433"
	ErrUnavailResource  = "437"
	ErrNotRegistered    = "451"
	ErrPasswdMismatch   = "464"
	ErrYoureBannedCreep = "465"
	ErrChannelIsFull    = "471"
	ErrInviteOnlyChan   = "473"
	ErrBannedFromChan   = "474"
	ErrBadChannelKey    = "475"
	ErrBadChanMask      = "476"
)

// IRC commands turborg sends or recognizes on the wire.
const (
	CmdPrivmsg      = "PRIVMSG"
	CmdNotice       = "NOTICE"
	CmdJoin         = "JOIN"
	CmdPart         = "PART"
	CmdQuit         = "QUIT"
	CmdNick         = "NICK"
	CmdUser         = "USER"
	CmdPass         = "PASS"
	CmdPing         = "PING"
	CmdPong         = "PONG"
	CmdMode         = "MODE"
	CmdTopic        = "TOPIC"
	CmdKick         = "KICK"
	CmdNames        = "NAMES"
	CmdCap          = "CAP"
	CmdWhois        = "WHOIS"
	CmdInvite       = "INVITE"
	CmdAway         = "AWAY"
	CmdList         = "LIST"
	CmdWho          = "WHO"
	CmdAuthenticate = "AUTHENTICATE"
	CmdError        = "ERROR"
	// CmdChathistory is the IRCv3 draft/chathistory command. Handled
	// locally by the bouncer (queries messages.Store, returns a
	// chathistory-typed BATCH); never forwarded upstream.
	CmdChathistory = "CHATHISTORY"
	// CmdBatch is the IRCv3 batch command. Used by the bouncer when
	// framing CHATHISTORY responses and welcome replay for
	// batch-capable clients.
	CmdBatch = "BATCH"
	// CmdTB is the turborg meta-command. Handled locally by the bouncer
	// and web gateway; never forwarded upstream.
	CmdTB = "TB"
)

// IsHandshakeComplete reports whether the given numeric reply signals the
// server has finished sending its MOTD and the connection is ready for
// normal traffic. Either RPL_ENDOFMOTD (376) or ERR_NOMOTD (422)
// completes the handshake; servers without a MOTD send the latter.
func IsHandshakeComplete(numeric string) bool {
	return numeric == RplEndOfMOTD || numeric == ErrNoMOTD
}

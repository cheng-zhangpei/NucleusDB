package msg

// AgentMsg define the communication diagram between the agent
type AgentMsg struct {
	msg  string
	from uint64
	to   uint64
}

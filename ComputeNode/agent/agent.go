package agent

type agent struct {
	agentId string

	privateMemSpaceKeys []string
	sharedMemSpaceKeys  []string
}

func NewAgent() *agent {
	return &agent{}
}

// -------------------define the action of agent----------------------

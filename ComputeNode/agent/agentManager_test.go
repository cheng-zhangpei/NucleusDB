package agent

import (
	"ComputeNode/memspace"
	_ "ComputeNode/memspace"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_AgentManager(t *testing.T) {
	client := memspace.NewNucleusClient("127.0.0.1:31001", 1)
	var privatePath string = "/NucleusDB/vector/private"
	var publicPath string = "/NucleusDB/vector/public"
	var metaPath string = "/NucleusDB/vector/meta"
	manager, err := memspace.NewMemSpaceManager(client, privatePath, publicPath, metaPath)
	if err != nil {
		t.Fatal(err)
	}

	AgentManager := NewAgentManager(manager)
	err = AgentManager.RegisterInternalAgent(1, "http://localhost:20001", "http://localhost:20002")
	assert.NoError(t, err)
	err = AgentManager.RegisterInternalAgent(2, "http://localhost:20001", "http://localhost:20002")
	assert.NoError(t, err)
	// 创建测试记忆空间
	err = AgentManager.mmManager.RegisterMemSpace(1, memspace.Private, 10000,
		memspace.ContentMemory, "http://localhost:20002")
	assert.NoError(t, err)

	err = AgentManager.mmManager.RegisterMemSpace(2, memspace.Private, 10000,
		memspace.ContentMemory, "http://localhost:20002")
	assert.NoError(t, err)

	err = AgentManager.mmManager.RegisterMemSpace(1, memspace.Shared, 10000,
		memspace.ContentMemory, "http://localhost:20002")
	assert.NoError(t, err)

	// 常规绑定测试
	// private 卷绑定测试
	err = AgentManager.BindingPrivateMemSpace(1, 1)
	assert.NoError(t, err)
	err = AgentManager.BindingPrivateMemSpace(2, 2)
	assert.NoError(t, err)
	// share 卷绑定测试
	err = AgentManager.BindingPublicMemSpace(1, 1)
	assert.NoError(t, err)
	err = AgentManager.BindingPublicMemSpace(2, 1)
	assert.NoError(t, err)

}

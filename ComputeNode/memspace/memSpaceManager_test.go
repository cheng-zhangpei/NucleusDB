package memspace

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMemSpaceManager(t *testing.T) {
	client := NewNucleusClient("127.0.0.1:31001", 1)
	var privatePath string = "/NucleusDB/vector/private"
	var publicPath string = "/NucleusDB/vector/public"
	var metaPath string = "/NucleusDB/vector/meta"
	manager, err := NewMemSpaceManager(client, privatePath, publicPath, metaPath)
	assert.NoError(t, err)
	// 测试加载数据
	err = manager.RegisterMemSpace(1, Private, 10000, ContentMemory, "http://localhost:5000")
	assert.NoError(t, err)

	err = manager.loadMemSpace(1)
	assert.NoError(t, err)

	err = manager.clearMemSpace(1)
	assert.NoError(t, err)

	err = manager.loadMemSpace(1)
	assert.Error(t, err)
}

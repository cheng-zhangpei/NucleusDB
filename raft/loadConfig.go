package raft

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

// LoadConfig 使用 yaml.v2 解析 YAML 文件
func LoadConfig(configPath string) (*RaftConfig, error) {
	// 读取 YAML 文件内容
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// 初始化配置结构体
	config := &RaftConfig{}

	// 解析 YAML 数据到结构体
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return config, nil
}

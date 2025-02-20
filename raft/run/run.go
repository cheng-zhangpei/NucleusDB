package run

import (
	"ComDB"
	"ComDB/raft"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// 在分布式环境下 运行节点
func main() {
	// 初始化配置
	log.Println("Starting ComDB raft server...............")
	config, err := raft.LoadConfigWithEnv("../configs/config.yaml") // 需要实现环境变量加载逻辑
	if err != nil {
		panic(err)
	}
	// 把config给打印出来
	config.Print()
	// 数据库配置,emmm,暂时数据库配置先不开放了哈哈哈我想偷懒直接在内部给写完就好了
	options := ComDB.DefaultOptions
	err = os.Mkdir("./data", os.ModePerm)
	// 启动 Raft 节点
	options.DirPath = "./data"
	raft.StartNode(config, options)
	// 优雅的关闭呵呵呵笑死
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-termChan:
		return
	}
}

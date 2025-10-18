package main

import (
	"NucleusDB"
	"NucleusDB/raft"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// 在分布式环境下 运行节点
// 注意一下main方法的工作路径一定是在NucleusDB下的
func main() {
	startNode("./data1", "../configs/raft_config_1.yaml")
}
func startNode(dataDir string, configPath string) {
	// 初始化配置
	printBanner()
	log.Println("Starting NucleusDB raft server...............")
	wd, err := os.Getwd()
	if err != nil {
		fmt.Printf("Failed to get current working directory: %v\n", err)
		return
	}
	fmt.Printf("Current working directory: %s\n", wd)
	//config, err := raft.LoadConfigWithEnv("../configs/raft_config_1.yaml") // 需要实现环境变量加载逻辑
	config, err := raft.LoadConfigWithEnv(configPath) // 需要实现环境变量加载逻辑

	if err != nil {
		panic(err)
	}
	// 把config给打印出来
	config.Print()
	// 数据库配置,emmm,暂时数据库配置先不开放了哈哈哈我想偷懒直接在内部给写完就好了
	options := NucleusDB.DefaultOptions
	//err = os.Mkdir("./data1", os.ModePerm)
	err = os.Mkdir(dataDir, os.ModePerm)
	// 启动 Raft 节点
	options.DirPath = dataDir
	raft.StartNode(config, options)
	// 说白了就是收到  1信号就智暂停咯，很好理解
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-termChan:
		return
	}
}

func printBanner() {
	newBanner := "       ,--.'|                             ,--,                                               ,---,         ,---,.  \n   ,--,:  : |                           ,--.'|                                             .'  .' `\\     ,'  .'  \\ \n,`--.'`|  ' :          ,--,             |  | :                        ,--,               ,---.'     \\  ,---.' .' | \n|   :  :  | |        ,'_ /|             :  : '                      ,'_ /|    .--.--.    |   |  .`\\  | |   |  |: | \n:   |   \\ | :   .--. |  | :     ,---.   |  ' |       ,---.     .--. |  | :   /  /    '   :   : |  '  | :   :  :  / \n|   : '  '; | ,'_ /| :  . |    /     \\  '  | |      /     \\  ,'_ /| :  . |  |  :  /`./   |   ' '  ;  : :   |    ;  \n'   ' ;.    ; |  ' | |  . .   /    / '  |  | :     /    /  | |  ' | |  . .  |  :  ;_     '   | ;  .  | |   :     \\ \n|   | | \\   | |  | ' |  | |  .    ' /   '  : |__  .    ' / | |  | ' |  | |   \\  \\    `.  |   | :  |  ' |   |   . | \n'   : |  ; .' :  | : ;  ; |  '   ; :__  |  | '.'| '   ;   /| :  | : ;  ; |    `----.   \\ '   : | /  ;  '   :  '; | \n|   | '`--'   '  :  `--'   \\ '   | '.'| ;  :    ; '   |  / | '  :  `--'   \\  /  /`--'  / |   | '` ,/   |   |  | ;  \n'   : |       :  ,      .-./ |   :    : |  ,   /  |   :    | :  ,      .-./ '--'.     /  ;   :  .'     |   :   /   \n;   |.'        `--`----'      \\   \\  /   ---`-'    \\   \\  /   `--`----'       `--'---'   |   ,.'       |   | ,'    \n'---'                          `----'               `----'                               '---'         `----'    "
	banner :=
		"\nNucleusDB - A Raft-based Distributed Database focus on LLM memory management\n" +
			"Author: ZhangPei Cheng\n" +
			"source code: https://github.com/cheng-zhangpei/NucleusDB\n" +
			"--------------------------------------------------\n"
	fmt.Print(newBanner)
	fmt.Print(banner)
}

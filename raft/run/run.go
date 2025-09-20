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
	// 初始化配置
	printBanner()
	log.Println("Starting NucleusDB raft server...............")
	wd, err := os.Getwd()
	if err != nil {
		fmt.Printf("Failed to get current working directory: %v\n", err)
		return
	}
	fmt.Printf("Current working directory: %s\n", wd)
	config, err := raft.LoadConfigWithEnv("../configs/raft_config_1.yaml") // 需要实现环境变量加载逻辑
	if err != nil {
		panic(err)
	}
	// 把config给打印出来
	config.Print()
	// 数据库配置,emmm,暂时数据库配置先不开放了哈哈哈我想偷懒直接在内部给写完就好了
	options := NucleusDB.DefaultOptions
	err = os.Mkdir("./data1", os.ModePerm)
	// 启动 Raft 节点
	options.DirPath = "./data1"
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
	banner := "                                                                                      \n" +
		"                ,----..              ____                             \n" +
		"  ,----..      /   /   \\           ,'  , `.     ,---,         ,---,. \n" +
		" /   /   \\    /   .     :       ,-+-,.' _ |   .'  .' `\\     ,'  .'  \\ \n" +
		"|   :     :  .   /   ;.  \\   ,-+-. ;   , || ,---.'     \\  ,---.' .' | \n" +
		".   |  ;. / .   ;   /  ` ;  ,--.'|'   |  ;| |   |  .`\\  | |   |  |: | \n" +
		".   ; /--`  ;   |  ; \\ ; | |   |  ,', |  ': :   : |  '  | :   :  :  / \n" +
		";   | ;     |   :  | ; | ' |   | /  | |  || |   ' '  ;  : :   |    ;  \n" +
		"|   : |     .   |  ' ' ' : '   | :  | :  |, '   | ;  .  | |   :     \\ \n" +
		".   | '___  '   ;  \\; /  | ;   . |  ; |--'  |   | :  |  ' |   |   . | \n" +
		"'   ; : .'|  \\   \\  ',  /  |   : |  | ,     '   : | /  ;  '   :  '; | \n" +
		"'   | '/  :   ;   :    /   |   : '  |/      |   | '` ,/   |   |  | ; \n" +
		"|   :    /     \\   \\ .'    ;   | |`-'       ;   :  .'     |   :   /   \n" +
		"\\   \\ .'       `---`      |   ;/           |   ,.'       |   | ,'    \n" +
		" `---`                    '---'            '---'         `----'      \n" +
		"\n" +
		"NucleusDB - A Raft-based Distributed Database focus on LLM memory management\n" +
		"Author: ZhangPei Cheng\n" +
		"source code: https://github.com/cheng-zhangpei/NucleusDB\n" +
		"--------------------------------------------------\n"

	fmt.Print(banner)
}

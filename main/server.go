package main

import (
	bitcask "NucleusDB"
	bitcask_redis "NucleusDB/main/redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

var addr = "127.0.0.1:6380"

type BitcaskServer struct {
	dbs    map[int]*bitcask_redis.RedisDataStructure
	server *redcon.Server
	mu     sync.RWMutex
	opts   bitcask.ServerConfig
}

// 其实这个地方有一个比较有意思的想法就是多个数据库实例同时启动到不同的地方？把数据库改成成多实例系统的可行性？
func main() {
	// 打开 Redis 数据结构服务
	redisDataStructure, err :=
		bitcask_redis.NewRedisDataStructure(bitcask.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// 初始化 BitcaskServer
	opts := bitcask.DefaultWebServerOptions
	bitcaskServer := &BitcaskServer{
		dbs:  make(map[int]*bitcask_redis.RedisDataStructure),
		opts: opts,
	}
	// 设置其中一个dbs实例，也就是redis实例
	bitcaskServer.dbs[0] = redisDataStructure
	// 初始化一个 Redis 服务端
	addr = bitcaskServer.opts.Host + ":" + bitcaskServer.opts.Port
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand,
		bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.listen()

}

func (svr *BitcaskServer) listen() {
	log.Printf("bitcask server running, ready to accept connections.")
	_ = svr.server.ListenAndServe()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true
}

func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
}

//func main() {
//	conn, err := net.Dial("tcp", "localhost:6379")
//	if err != nil {
//		panic(err)
//	}
//
//	// 向 Redis 发送一个命令
//	cmd := "set k-name-2 bitcask-kv-2\r\n"
//	conn.Write([]byte(cmd))
//
//	// 解析 Redis 响应
//	reader := bufio.NewReader(conn)
//	res, err := reader.ReadString('\n')
//	if err != nil {
//		panic(err)
//	}
//	fmt.Printf(res)
//}// redis 协议解析的示例

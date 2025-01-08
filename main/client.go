package main

// 注意本部分的内容

import (
	bitcask "ComDB"
	bitcask_redis "ComDB/main/redis"
	"ComDB/utils"
	"errors"
	"fmt"
	"github.com/tidwall/redcon"
	"strings"
	"time"
)

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	"set":   set,
	"get":   get,
	"hset":  hset,
	"sadd":  sadd,
	"lpush": lpush,
	"zadd":  zadd,
}

type BitcaskClient struct {
	server *BitcaskServer
	db     *bitcask_redis.RedisDataStructure
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))

	client, _ := conn.Context().(*BitcaskClient)
	switch command {
	case "quit":
		_ = conn.Close() // server response!
	case "ping":
		conn.WriteString("PONG")
	default:
		// 这里只有这里支持的命令
		cmdFunc, ok := supportedCommands[command]
		if !ok {
			conn.WriteError("Err unsupported command: '" + command + "'")
			return
		}
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if errors.Is(err, bitcask.ErrKeyNotFound) {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)
	}
}

// ================================所支持解析方法====================================

func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("set")
	}

	key, value := args[0], args[1]
	if err := cli.db.Set(key, time.Second*10000, value); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("get")
	}

	value, err := cli.db.Get(args[0])
	if err != nil {
		return nil, err
	}
	return value, nil
}

func hset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("hset")
	}

	var ok = 0
	key, field, value := args[0], args[1], args[2]
	res, err := cli.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func sadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("sadd")
	}

	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func lpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("lpush")
	}

	key, value := args[0], args[1]
	res, err := cli.db.LPush(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

func zadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("zadd")
	}

	var ok = 0
	key, score, member := args[0], args[1], args[2]
	res, err := cli.db.ZAdd(key, utils.FloatFromBytes(score), member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

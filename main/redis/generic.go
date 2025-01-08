package redis

import "errors"

// 用于存储各个数据类型的通用命令

func (rds *RedisDataStructure) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *RedisDataStructure) Type(key []byte) (RedisDataType, error) {
	// 把数据拿出来，第一个字节就是相关的内容了
	encValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encValue) == 0 {
		return 0, errors.New("value is empty")
	}
	return encValue[0], nil
}

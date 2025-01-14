package search

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// 记录搜索信息详细的编码和解码过程与数据格式
type SearchRecord struct {
	matchField   map[string]float64 // 使用 map 存储 TF-IDF 数据
	dataField    []byte
	simFlags     uint8     // 用于记录一些similarities 状态 7~1 bit 用于记录similarities的位置，0位记录similarities是否满
	similarities []float64 // 使用 map 存储 TF-IDF 数据，长度限制为127
}

func NewSearchRecord(comPressNumThreshold int64) *SearchRecord {
	return &SearchRecord{
		matchField:   make(map[string]float64),
		dataField:    make([]byte, 0),
		simFlags:     0, // 0: 未满, 初始位置位0 from [0,1,2....],此处在后续使用的时候需要注意上限
		similarities: make([]float64, comPressNumThreshold),
	}
}

func getSearchKey(timeStamp int64, agentId string) []byte {
	// 计算 agentId 的长度
	agentIdSize := len(agentId)

	// 计算缓冲区大小
	// timeStamp (8字节) + agentId 长度 (变长) + agentId 内容
	bufSize := 8 + binary.MaxVarintLen64 + agentIdSize
	buf := make([]byte, bufSize)

	index := 0

	// 编码 timeStamp（8字节，小端存储）
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(timeStamp))
	index += 8

	// 编码 agentId 的长度（变长编码）
	index += binary.PutUvarint(buf[index:], uint64(agentIdSize))

	// 编码 agentId 的内容
	copy(buf[index:index+agentIdSize], agentId)
	index += agentIdSize

	// 返回实际使用的字节数据
	return buf[:index]
}

// decodeSearchKey 解码字节数组，提取 timeStamp 和 agentId
func decodeSearchKey(data []byte) (int64, string, error) {
	index := 0

	// 解码 timeStamp（前8字节，小端存储）
	if len(data) < 8 {
		return 0, "", fmt.Errorf("invalid data: insufficient length for timestamp")
	}
	timeStamp := int64(binary.LittleEndian.Uint64(data[index : index+8]))
	index += 8

	// 解码 agentId 的长度（变长编码）
	agentIdSize, n := binary.Uvarint(data[index:])
	if n <= 0 {
		return 0, "", fmt.Errorf("invalid data: failed to decode agentId size")
	}
	index += n

	// 解码 agentId 的内容
	if len(data) < index+int(agentIdSize) {
		return 0, "", fmt.Errorf("invalid data: insufficient length for agentId")
	}
	agentId := string(data[index : index+int(agentIdSize)])

	return timeStamp, agentId, nil
}
func (sr *SearchRecord) Encode() []byte {
	// 将 matchField 序列化为 JSON
	matchFieldJSON, err := json.Marshal(sr.matchField)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal matchField: %v", err))
	}

	// 将 similarities 序列化为 JSON
	similaritiesJSON, err := json.Marshal(sr.similarities)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal similarities: %v", err))
	}

	// 计算 matchField、similarities 和 dataField 的长度
	matchFieldSize := len(matchFieldJSON)
	similaritiesSize := len(similaritiesJSON)
	dataFieldSize := len(sr.dataField)

	// 计算缓冲区大小
	// matchField 长度 (变长) + matchField 内容 + similarities 长度 (变长) + similarities 内容 + dataField 长度 (变长) + dataField 内容 + simFlags (1字节)
	bufSize := binary.MaxVarintLen64 + matchFieldSize + binary.MaxVarintLen64 + similaritiesSize + binary.MaxVarintLen64 + dataFieldSize + 1
	buf := make([]byte, bufSize)

	index := 0

	// 编码 matchField 的长度（变长编码）
	index += binary.PutUvarint(buf[index:], uint64(matchFieldSize))

	// 编码 matchField 的内容
	copy(buf[index:index+matchFieldSize], matchFieldJSON)
	index += matchFieldSize

	// 编码 similarities 的长度（变长编码）
	index += binary.PutUvarint(buf[index:], uint64(similaritiesSize))

	// 编码 similarities 的内容
	copy(buf[index:index+similaritiesSize], similaritiesJSON)
	index += similaritiesSize

	// 编码 dataField 的长度（变长编码）
	index += binary.PutUvarint(buf[index:], uint64(dataFieldSize))

	// 编码 dataField 的内容
	copy(buf[index:index+dataFieldSize], sr.dataField)
	index += dataFieldSize

	// 编码 simFlags
	buf[index] = sr.simFlags
	index += 1

	// 返回实际使用的字节数据
	return buf[:index]
}
func DecodeSearchRecord(data []byte) (*SearchRecord, error) {
	sr := &SearchRecord{}
	index := 0

	// 解码 matchField 的长度（变长编码）
	matchFieldSize, n := binary.Uvarint(data[index:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid data: failed to decode matchField size")
	}
	index += n

	// 解码 matchField 的内容
	if len(data) < index+int(matchFieldSize) {
		return nil, fmt.Errorf("invalid data: insufficient bytes for matchField")
	}
	matchFieldJSON := data[index : index+int(matchFieldSize)]
	index += int(matchFieldSize)

	// 将 matchField 反序列化为 map
	sr.matchField = make(map[string]float64)
	if err := json.Unmarshal(matchFieldJSON, &sr.matchField); err != nil {
		return nil, fmt.Errorf("failed to unmarshal matchField: %v", err)
	}

	// 解码 similarities 的长度（变长编码）
	similaritiesSize, n := binary.Uvarint(data[index:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid data: failed to decode similarities size")
	}
	index += n

	// 解码 similarities 的内容
	if len(data) < index+int(similaritiesSize) {
		return nil, fmt.Errorf("invalid data: insufficient bytes for similarities")
	}
	similaritiesJSON := data[index : index+int(similaritiesSize)]
	index += int(similaritiesSize)

	// 将 similarities 反序列化为 []float64
	sr.similarities = make([]float64, 0)
	if err := json.Unmarshal(similaritiesJSON, &sr.similarities); err != nil {
		return nil, fmt.Errorf("failed to unmarshal similarities: %v", err)
	}

	// 解码 dataField 的长度（变长编码）
	dataFieldSize, n := binary.Uvarint(data[index:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid data: failed to decode dataField size")
	}
	index += n

	// 解码 dataField 的内容
	if len(data) < index+int(dataFieldSize) {
		return nil, fmt.Errorf("invalid data: insufficient bytes for dataField")
	}
	sr.dataField = make([]byte, dataFieldSize)
	copy(sr.dataField, data[index:index+int(dataFieldSize)])
	index += int(dataFieldSize)

	// 解码 simFlags
	if len(data) < index+1 {
		return nil, fmt.Errorf("invalid data: insufficient bytes for simFlags")
	}
	sr.simFlags = data[index]
	index += 1

	return sr, nil
}

// statusDecode 用于状态的解码
func statusDecode(flag uint8) (index int, isFull bool) {
	// 第 0 位表示是否已满
	isFull = (flag & 0x01) == 1
	// 第 1~7 位表示当前更新位置
	index = int((flag >> 1) & 0x7F)
	return index, isFull
}

// statusEncode 用于状态的编码
func statusEncode(index int, isFull bool) uint8 {
	// 第 0 位表示是否已满
	var flag uint8
	if isFull {
		flag |= 0x01
	}

	// 第 1~7 位表示当前更新位置
	flag |= uint8((index & 0x7F) << 1)

	return flag
}

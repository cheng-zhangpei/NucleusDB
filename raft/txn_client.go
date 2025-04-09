package raft

// TxnClient todo: 对分布式连续事务进行封装，避免因为事务请求间隔过长而影响事务的吞吐，否则事务会变成类似串行话的事务机制,严重影响效率
type TxnClient struct {
	// 客户端需要指定一个节点提交事务数据
	node *node
}

func NewTxnClient(n *node) *TxnClient {
	return &TxnClient{n}
}

// todo 后面的Get和Set等方法皆是通过发送请求的方式将数据发送给

func (t *TxnClient) Set(key, value string) {

}
func (t *TxnClient) Get(key string) string {
	return ""
}

func (t *TxnClient) Delete(key string) {

}
func (t *TxnClient) Update() {

}

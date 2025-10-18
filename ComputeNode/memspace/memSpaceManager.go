package memspace

type MemSpaceManager struct {
	// todo 这两张表一定是惰性加载记忆空间的内容，最好是设计一下容量来着

	// private memspace
	privateTable map[string]*MemSpace
	// share memsapce
	publicTable map[string]*MemSpace
}

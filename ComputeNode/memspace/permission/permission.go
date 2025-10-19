package permission

// MemSpacePermission defines the types of permissions for memory space access
type MemSpacePermission int

const (
	// PermissionNone - No access permissions
	PermissionNone MemSpacePermission = iota

	// PermissionRead - Read access to memory space content
	PermissionRead

	// PermissionWrite - Write access to add new memories
	PermissionWrite

	// PermissionUpdate - Update access to modify existing memories
	PermissionUpdate

	// PermissionDelete - Delete access to remove memories
	PermissionDelete

	// PermissionCompress - Permission to perform memory compression operations
	PermissionCompress

	// PermissionShare - Permission to share this memory space with other agents
	PermissionShare

	// PermissionAdmin - Administrative privileges including all permissions and ability to manage permissions
	PermissionAdmin
)

// PermissionSet represents a collection of permissions using bitmask for multi-permission combinations
type PermissionSet int

const (
	// Basic permission combinations
	ReadOnly   PermissionSet = 1 << PermissionRead
	ReadWrite  PermissionSet = 1<<PermissionRead | 1<<PermissionWrite
	ReadUpdate PermissionSet = 1<<PermissionRead | 1<<PermissionUpdate

	// Full user permissions (excluding admin privileges)
	FullUser PermissionSet = 1<<PermissionRead | 1<<PermissionWrite |
		1<<PermissionUpdate | 1<<PermissionDelete |
		1<<PermissionCompress

	// Administrator permissions (includes all permissions except None)
	Admin PermissionSet = 1<<PermissionRead | 1<<PermissionWrite |
		1<<PermissionUpdate | 1<<PermissionDelete |
		1<<PermissionCompress | 1<<PermissionShare |
		1<<PermissionAdmin
)

// AgentPermission records permission assignments for agents
type AgentPermission struct {
	AgentID     string        // Unique identifier for the agent
	MemSpaceKey string        // Key of the memory space
	Permissions PermissionSet // Bitmask of granted permissions
	GrantedAt   int64         // Timestamp when permissions were granted
	ExpiresAt   int64         // Expiration timestamp (0 means never expires)
}

// HasPermission checks if the permission set includes a specific permission
func (p PermissionSet) HasPermission(perm MemSpacePermission) bool {
	return p&(1<<perm) != 0
}

// AddPermission adds a permission to the permission set
func (p *PermissionSet) AddPermission(perm MemSpacePermission) {
	*p |= 1 << perm
}

// RemovePermission removes a permission from the permission set
func (p *PermissionSet) RemovePermission(perm MemSpacePermission) {
	*p &^= 1 << perm
}

// ToSlice converts the permission set to a slice of individual permissions
func (p PermissionSet) ToSlice() []MemSpacePermission {
	var perms []MemSpacePermission
	for i := PermissionNone; i <= PermissionAdmin; i++ {
		if p.HasPermission(i) {
			perms = append(perms, i)
		}
	}
	return perms
}

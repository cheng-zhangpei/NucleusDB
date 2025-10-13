package memspace

// PermissionManager manages access control for memory spaces
type PermissionManager struct {
	permissions map[string]*AgentPermission // key format: "agentID:memSpaceKey"
}

// NewPermissionManager creates a new PermissionManager instance
func NewPermissionManager() *PermissionManager {
	return &PermissionManager{
		permissions: make(map[string]*AgentPermission),
	}
}

// GrantPermission grants permissions to an agent for a specific memory space
func (pm *PermissionManager) GrantPermission(agentID, memSpaceKey string, perms PermissionSet, expiresAt int64) {
	key := pm.generateKey(agentID, memSpaceKey)
	pm.permissions[key] = &AgentPermission{
		AgentID:     agentID,
		MemSpaceKey: memSpaceKey,
		Permissions: perms,
		GrantedAt:   getCurrentTimestamp(),
		ExpiresAt:   expiresAt,
	}
}

// RevokePermission revokes all permissions from an agent for a memory space
func (pm *PermissionManager) RevokePermission(agentID, memSpaceKey string) {
	key := pm.generateKey(agentID, memSpaceKey)
	delete(pm.permissions, key)
}

// CheckPermission verifies if an agent has a specific permission for a memory space
func (pm *PermissionManager) CheckPermission(agentID, memSpaceKey string, perm MemSpacePermission) bool {
	key := pm.generateKey(agentID, memSpaceKey)
	agentPerm, exists := pm.permissions[key]
	if !exists {
		return false
	}

	// Check if permission has expired
	if agentPerm.ExpiresAt > 0 && getCurrentTimestamp() > agentPerm.ExpiresAt {
		delete(pm.permissions, key)
		return false
	}

	return agentPerm.Permissions.HasPermission(perm)
}

// GetAgentPermissions retrieves all permissions for an agent on a memory space
func (pm *PermissionManager) GetAgentPermissions(agentID, memSpaceKey string) PermissionSet {
	key := pm.generateKey(agentID, memSpaceKey)
	if agentPerm, exists := pm.permissions[key]; exists {
		// Check expiration
		if agentPerm.ExpiresAt > 0 && getCurrentTimestamp() > agentPerm.ExpiresAt {
			delete(pm.permissions, key)
			return 0
		}
		return agentPerm.Permissions
	}
	return 0
}

// ListMemSpaceAgents lists all agents with permissions for a memory space
func (pm *PermissionManager) ListMemSpaceAgents(memSpaceKey string) []string {
	var agents []string
	prefix := ":" + memSpaceKey

	for key, perm := range pm.permissions {
		// Check expiration
		if perm.ExpiresAt > 0 && getCurrentTimestamp() > perm.ExpiresAt {
			delete(pm.permissions, key)
			continue
		}

		if len(key) > len(prefix) && key[len(key)-len(prefix):] == prefix {
			agents = append(agents, perm.AgentID)
		}
	}
	return agents
}

// generateKey creates a unique key for storing permission mappings
func (pm *PermissionManager) generateKey(agentID, memSpaceKey string) string {
	return agentID + ":" + memSpaceKey
}

// getCurrentTimestamp returns the current timestamp
func getCurrentTimestamp() int64 {
	// In production, use time.Now().Unix()
	return 0 // Simplified implementation
}

// String returns the string representation of a permission
func (p MemSpacePermission) String() string {
	switch p {
	case PermissionNone:
		return "NONE"
	case PermissionRead:
		return "READ"
	case PermissionWrite:
		return "WRITE"
	case PermissionUpdate:
		return "UPDATE"
	case PermissionDelete:
		return "DELETE"
	case PermissionCompress:
		return "COMPRESS"
	case PermissionShare:
		return "SHARE"
	case PermissionAdmin:
		return "ADMIN"
	default:
		return "UNKNOWN"
	}
}

// String returns the string representation of a permission set
func (p PermissionSet) String() string {
	if p == 0 {
		return "NONE"
	}
	if p == Admin {
		return "ADMIN"
	}

	var result string
	for i := PermissionRead; i <= PermissionShare; i++ {
		if p.HasPermission(i) {
			if result != "" {
				result += "|"
			}
			result += i.String()
		}
	}
	return result
}

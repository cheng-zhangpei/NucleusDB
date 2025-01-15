# HTTP API Documentation

## 1. Bitcask Related APIs

### 1.1 PUT Request
- **Function**: Insert a key-value pair into the database.
- **Type**: `POST`
- **URL**: `/bitcask/put`
- **Parameters**:
  - `key`: Key (string)
  - `value`: Value (string)

### 1.2 GET Request
- **Function**: Retrieve a value by key.
- **Type**: `GET`
- **URL**: `/bitcask/get`
- **Parameters**:
  - `key`: Key (string)

### 1.3 DELETE Request
- **Function**: Delete a key-value pair by key.
- **Type**: `DELETE`
- **URL**: `/bitcask/delete`
- **Parameters**:
  - `key`: Key (string)

### 1.4 List All Keys
- **Function**: List all keys in the database.
- **Type**: `GET`
- **URL**: `/bitcask/listkeys`
- **Parameters**: None

### 1.5 Get Database Statistics
- **Function**: Retrieve database statistics.
- **Type**: `GET`
- **URL**: `/bitcask/stat`
- **Parameters**: None

### 1.6 Prefix Query
- **Function**: Query key-value pairs by prefix.
- **Type**: `GET`
- **URL**: `/bitcask/prefix`
- **Parameters**:
  - `prefix`: Prefix (string)

---

## 2. Memory Related APIs

### 2.1 Get Memory
- **Function**: Retrieve memory by `agentId`.
- **Type**: `GET`
- **URL**: `/memory/get`
- **Parameters**:
  - `agentId`: Agent ID (string)

### 2.2 Set Memory
- **Function**: Set memory.
- **Type**: `POST`
- **URL**: `/memory/set`
- **Parameters**:
  - `agentId`: Agent ID (string)
  - `value`: Memory content (string)

### 2.3 Search Memory
- **Function**: Search memory by `agentId` and `searchItem`.
- **Type**: `GET`
- **URL**: `/memory/search`
- **Parameters**:
  - `agentId`: Agent ID (string)
  - `searchItem`: Search content (string)

### 2.4 Create Memory Space
- **Function**: Create a new memory space.
- **Type**: `POST`
- **URL**: `/memory/create`
- **Parameters**:
  - `agentId`: Agent ID (string)
  - `totalSize`: Memory space size (integer)

### 2.5 Compress Memory
- **Function**: Compress memory space.
- **Type**: `POST`
- **URL**: `/memory/compress`
- **Parameters**:
  - `agentId`: Agent ID (string)
  - `endpoint`: Compression service endpoint (string)
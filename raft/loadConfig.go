package raft

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// LoadConfig 使用 yaml.v2 解析 YAML 文件
func LoadConfig(configPath string) (*RaftConfig, error) {
	// 读取 YAML 文件内容
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// 初始化配置结构体
	config := &RaftConfig{}

	// 解析 YAML 数据到结构体
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	return config, nil
}

// LoadConfigWithEnv 这个方法主要是用于设置了容器的环境变量的时候启动的时候可以加载容器的环境变量到现在的程序中
func LoadConfigWithEnv(configPath string) (*RaftConfig, error) {
	// 原有YAML加载逻辑
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	config := &RaftConfig{}
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// 新增环境变量处理
	if err := loadFromEnv(config); err != nil {
		return nil, err
	}

	return config, nil
}

// 将环境变量装载到配置中
func loadFromEnv(cfg *RaftConfig) error {
	val := reflect.ValueOf(cfg).Elem()
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		structField := typ.Field(i)

		envTag := structField.Tag.Get("env")
		if envTag == "" {
			continue
		}

		envValue := os.Getenv(envTag)
		if envValue == "" {
			continue
		}

		// 处理特殊字段类型
		switch field.Kind() {
		case reflect.Slice:
			separator := structField.Tag.Get("envSeparator")
			if separator == "" {
				separator = "," // 默认分隔符
			}
			parts := strings.Split(envValue, separator)
			field.Set(reflect.ValueOf(parts))
			continue
		}

		// 通用类型处理
		switch v := field.Interface().(type) {
		case uint64:
			parsed, err := strconv.ParseUint(envValue, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid value for %s: %w", envTag, err)
			}
			field.SetUint(parsed)
		case int:
			parsed, err := strconv.Atoi(envValue)
			if err != nil {
				return fmt.Errorf("invalid value for %s: %w", envTag, err)
			}
			field.SetInt(int64(parsed))
		case bool:
			parsed, err := parseBool(envValue)
			if err != nil {
				return fmt.Errorf("invalid bool value for %s: %w", envTag, err)
			}
			field.SetBool(parsed)
		case time.Duration:
			atoi, err := strconv.Atoi(envValue)
			if err != nil {
				return fmt.Errorf("invalid value for %s: %w", envTag, err)
			}
			field.SetInt(int64(atoi))
		case string:
			field.SetString(envValue)
		default:
			return fmt.Errorf("unsupported type for %s: %T", envTag, v)
		}
	}
	return nil
}

// 增强的布尔值解析
func parseBool(s string) (bool, error) {
	switch strings.ToLower(s) {
	case "true", "1", "yes", "on":
		return true, nil
	case "false", "0", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean value: %s", s)
	}
}
func (c *RaftConfig) Print() {
	val := reflect.ValueOf(c).Elem()
	typ := val.Type()

	// 计算字段名和字段值的最大长度
	maxFieldNameLength := 0
	maxFieldValueLength := 0
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		structField := typ.Field(i)

		if !field.CanInterface() {
			continue
		}

		// 计算字段名的长度
		fieldNameLength := len(structField.Name)
		if fieldNameLength > maxFieldNameLength {
			maxFieldNameLength = fieldNameLength
		}

		// 计算字段值的长度
		fieldValue := getFormattedValue(field)
		fieldValueLength := len(fieldValue)
		if fieldValueLength > maxFieldValueLength {
			maxFieldValueLength = fieldValueLength
		}
	}

	// 动态调整表格宽度
	tableWidth := maxFieldNameLength + maxFieldValueLength + 7 // 7 是固定字符（边框、冒号、空格等）
	if tableWidth < 30 {                                       // 最小宽度
		tableWidth = 30
	}

	// 打印表格头部
	printTableHeader(tableWidth)

	// 打印字段信息
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		structField := typ.Field(i)

		if !field.CanInterface() {
			continue
		}

		fieldName := fmt.Sprintf("%-*s", maxFieldNameLength, structField.Name)
		fieldValue := getFormattedValue(field)

		// 打印一行
		fmt.Printf("║ \033[36m%s\033[0m : %-*s ║\n", fieldName, maxFieldValueLength, fieldValue)
	}

	// 打印表格底部
	printTableFooter(tableWidth)
}

// 打印表格头部
func printTableHeader(width int) {
	fmt.Printf("╔%s╗\n", strings.Repeat("═", width-2))
	fmt.Printf("║%*s║\n", (width+len("Raft Configuration"))/2, "Raft Configuration")
	fmt.Printf("╠%s╣\n", strings.Repeat("═", width-2))
}

// 打印表格底部
func printTableFooter(width int) {
	fmt.Printf("╚%s╝\n", strings.Repeat("═", width-2))
}

// 格式化字段值
func getFormattedValue(field reflect.Value) string {
	switch v := field.Interface().(type) {
	case time.Duration:
		return v.String()
	case []string:
		return "[" + strings.Join(v, ", ") + "]"
	case bool:
		return fmt.Sprintf("%t", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

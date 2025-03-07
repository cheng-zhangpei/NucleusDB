# 构建阶段：使用 Go 1.22 基础镜像
FROM golang:1.22-alpine AS builder

# 安装构建所需的依赖，包括 C 编译器
RUN apk update && apk add --no-cache build-base git

# 设置工作目录
WORKDIR /app

# 复制 go.mod 和 go.sum 并下载依赖
COPY go.mod go.sum ./
# 复制整个项目代码
COPY . .
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN go mod download
# 构建可执行文件，启用 CGO 并指定目标平台为 Linux
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o node ./raft/run

# 运行时阶段：使用轻量级 Alpine 镜像
FROM alpine:latest
# 安装 libc6-compat 和必要的 C++ 库
RUN apk add --no-cache libc6-compat libstdc++ libgcc
# 设置工作目录
WORKDIR /app
# 从构建阶段复制可执行文件
COPY --from=builder /app /app
# 进入 raft/run 目录
WORKDIR /app/raft/run
# 赋予可执行文件权限
RUN chmod +x node
# 暴露端口
EXPOSE 8080 5001

# 设置容器启动命令
CMD ["./node"]
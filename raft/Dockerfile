# 构建阶段：使用 Go 1.22 基础镜像
FROM golang:1.22-alpine AS builder

# 设置工作目录为项目根目录（注意路径层级）
WORKDIR /app

# 复制整个项目（包括 ComdDB 和 raft 目录）
COPY .. .

# 进入 raft/run 目录构建可执行文件
WORKDIR /app/raft/run

# 下载依赖并构建（Go 模块路径可能需要调整）
RUN go mod tidy -C ../../ComDB && \
    CGO_ENABLED=0 GOOS=linux go build -o node .&& \
    chmod +x node


# 运行时阶段：使用轻量级 Alpine 镜像
FROM alpine:latest

# 设置工作目录
WORKDIR /app

# 从构建阶段复制整个 ComDB 目录
COPY --from=builder /app /app

# 进入 raft/run 目录
WORKDIR /app/raft/run

# 赋予可执行文件权限
RUN chmod +x node

# 暴露端口 8080 ：httpserver  5001：grpc server
EXPOSE 8080 5001

# 设置容器启动命令
CMD ["./node"]
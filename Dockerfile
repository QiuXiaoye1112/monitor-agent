FROM alpine:3.21

WORKDIR /app

# Docker buildx 会在构建时自动填充这些变量
ARG TARGETOS
ARG TARGETARCH

COPY monitor-agent-${TARGETOS}-${TARGETARCH} /app/monitor-agent

RUN chmod +x /app/monitor-agent

RUN touch /.monitor-agent-container

ENTRYPOINT ["/app/monitor-agent"]
# 运行时请指定参数
# Please specify parameters at runtime.
# eg: docker run monitor-agent -e example.com -t token
CMD ["--help"]

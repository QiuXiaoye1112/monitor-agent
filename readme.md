# Monitor Agent

VPS 监控节点数据采集 Agent。

## 安装

```bash
curl -fsSL https://github.com/QiuXiaoye1112/monitor-agent/releases/download/v1.0.0/monitor-agent-linux-amd64 -o /usr/local/bin/monitor-agent
chmod +x /usr/local/bin/monitor-agent
/usr/local/bin/monitor-agent -e http://<中心主机IP>:25774 -t <Token> -i 1
```

## 注册系统服务

```bash
cat > /etc/systemd/system/monitor-agent.service << EOF
[Unit]
Description=Monitor Agent
After=network.target
[Service]
Type=simple
ExecStart=/usr/local/bin/monitor-agent -e http://<中心主机IP>:25774 -t <Token> -i 1
Restart=always
RestartSec=10
[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now monitor-agent
```

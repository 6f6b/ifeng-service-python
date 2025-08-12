# 使用官方Python镜像作为基础镜像，指定平台
FROM --platform=linux/arm64 python:3.9-slim

# 设置时区环境变量
ENV TZ=Asia/Shanghai

# 安装cron和tzdata
RUN apt-get update && apt-get install -y cron tzdata \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制项目文件
COPY requirements.txt .
COPY scripts/ ./scripts/
COPY config/crontab /etc/cron.d/python-cron
COPY . .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 设置crontab文件权限
RUN chmod 0644 /etc/cron.d/python-cron

# 创建日志目录
RUN mkdir -p /app/logs

# 创建空的cron日志文件
RUN touch /app/logs/cron.log

# 应用crontab
RUN crontab /etc/cron.d/python-cron

# 启动命令
CMD cron && tail -f /app/logs/cron.log 
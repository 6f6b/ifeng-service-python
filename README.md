# Python定时任务服务

这是一个基于Docker的Python定时任务服务项目。该项目可以管理和运行多个Python脚本，并按照预设的时间表自动执行。

## 项目结构

```
.
├── Dockerfile              # Docker镜像构建文件
├── requirements.txt        # Python依赖包
├── scripts/               # Python脚本目录
│   └── sample_task.py     # 示例任务脚本
├── config/                # 配置文件目录
│   └── crontab           # 定时任务配置文件
└── logs/                  # 日志目录
```

## 使用说明

1. 将需要执行的Python脚本放在 `scripts` 目录下
2. 在 `config/crontab` 文件中配置定时任务
3. 构建并运行Docker容器

### 构建Docker镜像
```bash
docker build -t ifeng-service-python .
```

### 运行容器
```bash
docker run -d --name ifeng-python-cron ifeng-service-python
``` 
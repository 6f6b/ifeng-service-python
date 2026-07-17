"""数据库连接配置（Docker / 本地 / RDS 均通过环境变量配置）"""
import os

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "rds.6f6b.cn"),
    "port": int(os.environ.get("DB_PORT", "3306")),
    "user": os.environ.get("DB_USER", "root"),
    "password": os.environ.get("DB_PASSWORD", "FuckTheHaker@666"),
    "database": os.environ.get("DB_NAME", "ifeng_research"),
    "charset": "utf8mb4",
}

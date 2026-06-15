"""数据库连接配置。默认连本地 Docker MySQL，同步线上时改环境变量即可。"""
import os

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "127.0.0.1"),
    "port": int(os.environ.get("DB_PORT", "3307")),
    "user": os.environ.get("DB_USER", "root"),
    "password": os.environ.get("DB_PASSWORD", "lf123456"),
    "database": os.environ.get("DB_NAME", "stock"),
    "charset": "utf8mb4",
}

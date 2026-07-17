"""Tushare 代理配置（环境变量可覆盖）"""
import os

import tushare as ts

TUSHARE_TOKEN = os.environ.get(
    "TUSHARE_TOKEN",
    "c8f6e3a592196b5fb7a4edee6e1c09faf001f38366f31a25d7661797",
)
TUSHARE_API_URL = os.environ.get(
    "TUSHARE_API_URL",
    "http://cheap-host1.cheapyun.com:24145",
)


def get_pro_api():
    url = TUSHARE_API_URL.rstrip("/") + "/"
    pro = ts.pro_api(TUSHARE_TOKEN)
    pro._DataApi__http_url = url
    return pro

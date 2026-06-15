"""Tushare 代理配置（与 Java stk/cb 的 application.properties 一致）"""
import os

import tushare as ts

TUSHARE_TOKEN = os.environ.get(
    "TUSHARE_TOKEN",
    "6a16dca8f45929a32761e63eecb58938fd508c670302089db380a4ea",
)
TUSHARE_API_URL = os.environ.get(
    "TUSHARE_API_URL",
    "http://121.41.121.204/q5ian/",
)


def get_pro_api():
    url = TUSHARE_API_URL.rstrip("/") + "/"
    pro = ts.pro_api(TUSHARE_TOKEN)
    pro._DataApi__http_url = url
    return pro

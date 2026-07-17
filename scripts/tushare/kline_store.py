"""daily_kline 批量写入工具"""
from __future__ import annotations

from typing import Iterable, Sequence

import pandas as pd

from db_util import batch_upsert

KLINE_COLUMNS = [
    "asset_type",
    "inst_id",
    "trade_date",
    "o",
    "h",
    "l",
    "c",
    "v",
    "amount",
]
KLINE_UPDATE_COLUMNS = ["o", "h", "l", "c", "v", "amount"]


def ts_code_to_inst_id(ts_code: str) -> str:
    if not ts_code:
        return ""
    dot = ts_code.find(".")
    return ts_code[:dot] if dot > 0 else ts_code


def trade_date_to_iso(trade_date: str) -> str:
    s = str(trade_date).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def _safe_float(v, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        x = float(v)
        return default if pd.isna(x) else x
    except (TypeError, ValueError):
        return default


def tushare_daily_to_kline_rows(
    asset_type: str,
    df: pd.DataFrame,
    ts_code_filter: set[str] | None = None,
) -> list[tuple]:
    if df is None or df.empty:
        return []
    rows: list[tuple] = []
    for _, data in df.iterrows():
        ts_code = str(data.get("ts_code", ""))
        if not ts_code:
            continue
        if ts_code_filter is not None and ts_code not in ts_code_filter:
            continue
        inst_id = ts_code_to_inst_id(ts_code)
        trade_date = trade_date_to_iso(str(data.get("trade_date", "")))
        o = _safe_float(data.get("open"))
        h = _safe_float(data.get("high"))
        l = _safe_float(data.get("low"))
        c = _safe_float(data.get("close"))
        vol_lots = _safe_float(data.get("vol"))
        amount_k = _safe_float(data.get("amount"))
        volume = int(round(vol_lots * 100))
        amount = amount_k * 1000
        if amount <= 0 and volume > 0 and c > 0:
            amount = c * volume
        rows.append((asset_type, inst_id, trade_date, o, h, l, c, volume, amount))
    return rows


def tushare_rt_to_kline_rows(df: pd.DataFrame, asset_type: str) -> list[tuple]:
    """rt_k 实时日线 → 当日 K 线"""
    if df is None or df.empty:
        return []
    today = pd.Timestamp.now(tz="Asia/Shanghai").strftime("%Y-%m-%d")
    rows: list[tuple] = []
    for _, data in df.iterrows():
        ts_code = str(data.get("ts_code", ""))
        if not ts_code:
            continue
        inst_id = ts_code_to_inst_id(ts_code)
        o = _safe_float(data.get("open"))
        h = _safe_float(data.get("high"))
        l = _safe_float(data.get("low"))
        c = _safe_float(data.get("close"))
        vol_lots = _safe_float(data.get("vol"))
        amount_k = _safe_float(data.get("amount"))
        volume = int(round(vol_lots * 100))
        amount = amount_k * 1000
        if amount <= 0 and volume > 0 and c > 0:
            amount = c * volume
        rows.append((asset_type, inst_id, today, o, h, l, c, volume, amount))
    return rows


def upsert_kline_rows(cursor, rows: Sequence[tuple], batch_size: int = 500) -> int:
    return batch_upsert(
        cursor,
        "daily_kline",
        KLINE_COLUMNS,
        rows,
        batch_size=batch_size,
        update_columns=KLINE_UPDATE_COLUMNS,
    )


def load_etf_ts_codes(cursor) -> set[str]:
    cursor.execute(
        "SELECT ts_code FROM instrument WHERE asset_type='etf' AND ts_code <> ''"
    )
    return {row[0] for row in cursor.fetchall() if row[0]}

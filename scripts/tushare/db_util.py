"""批量写入 MySQL 工具。"""
import time
from typing import Iterable, Sequence

import pandas as pd
import pymysql

from db_config import DB_CONFIG

DEFAULT_BATCH_SIZE = 500


def get_connection():
    return pymysql.connect(**DB_CONFIG)


def df_to_tuples(df: pd.DataFrame, columns: Sequence[str]) -> list[tuple]:
    rows = []
    for row in df[columns].itertuples(index=False, name=None):
        cleaned = []
        for val in row:
            if pd.isna(val):
                cleaned.append(None)
            else:
                cleaned.append(val)
        rows.append(tuple(cleaned))
    return rows


def batch_upsert(
    cursor,
    table: str,
    columns: Sequence[str],
    rows: Sequence[tuple],
    batch_size: int = DEFAULT_BATCH_SIZE,
    update_columns: Sequence[str] | None = None,
) -> int:
    if not rows:
        return 0

    quoted_cols = ", ".join(f"`{c}`" if c in ("open", "close", "change") else c for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    update_cols = list(update_columns or columns)
    update_clause = ", ".join(
        f"`{c}`=VALUES(`{c}`)" if c in ("open", "close", "change") else f"{c}=VALUES({c})"
        for c in update_cols
    )
    sql = (
        f"INSERT INTO {table} ({quoted_cols}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_clause}"
    )

    total = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        cursor.executemany(sql, chunk)
        total += len(chunk)
    return total


def batch_upsert_df(
    cursor,
    table: str,
    df: pd.DataFrame,
    columns: Sequence[str],
    batch_size: int = DEFAULT_BATCH_SIZE,
    update_columns: Sequence[str] | None = None,
) -> int:
    rows = df_to_tuples(df, columns)
    return batch_upsert(cursor, table, columns, rows, batch_size, update_columns)


def timed_batch_upsert_df(cursor, table, df, columns, logger, batch_size=DEFAULT_BATCH_SIZE):
    t0 = time.perf_counter()
    count = batch_upsert_df(cursor, table, df, columns, batch_size=batch_size)
    sec = time.perf_counter() - t0
    per_row_ms = (sec / count * 1000) if count else 0
    logger.info(
        f"[耗时] 批量入库: {sec:.2f}s，{count} 条，均 {per_row_ms:.2f}ms/条，batch={batch_size}"
    )
    return count, sec

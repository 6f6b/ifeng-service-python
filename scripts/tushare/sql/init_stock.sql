CREATE DATABASE IF NOT EXISTS stock
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE stock;

CREATE TABLE IF NOT EXISTS stock_basic (
    ts_code     VARCHAR(16)  NOT NULL COMMENT 'TS代码',
    symbol      VARCHAR(16)  NOT NULL COMMENT '股票代码',
    name        VARCHAR(64)  NOT NULL COMMENT '股票名称',
    area        VARCHAR(32)  NULL COMMENT '地域',
    industry    VARCHAR(64)  NULL COMMENT '所属行业',
    market      VARCHAR(32)  NULL COMMENT '市场类型',
    exchange    VARCHAR(16)  NULL COMMENT '交易所',
    curr_type   VARCHAR(8)   NULL COMMENT '交易货币',
    list_status CHAR(1)      NULL COMMENT '上市状态 L/D/P',
    list_date   VARCHAR(8)   NULL COMMENT '上市日期',
    delist_date VARCHAR(8)   NULL COMMENT '退市日期',
    is_hs       CHAR(1)      NULL COMMENT '沪深港通标的',
    PRIMARY KEY (ts_code),
    KEY idx_symbol (symbol),
    KEY idx_list_status (list_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票基础信息';

CREATE TABLE IF NOT EXISTS stock_daily (
    ts_code     VARCHAR(16)    NOT NULL COMMENT 'TS代码',
    trade_date  VARCHAR(8)     NOT NULL COMMENT '交易日期 YYYYMMDD',
    `open`      DECIMAL(12,4)  NULL COMMENT '开盘价',
    high        DECIMAL(12,4)  NULL COMMENT '最高价',
    low         DECIMAL(12,4)  NULL COMMENT '最低价',
    `close`     DECIMAL(12,4)  NULL COMMENT '收盘价',
    pre_close   DECIMAL(12,4)  NULL COMMENT '昨收价',
    `change`    DECIMAL(12,4)  NULL COMMENT '涨跌额',
    pct_chg     DECIMAL(10,4)  NULL COMMENT '涨跌幅(%)',
    vol         DECIMAL(20,2)  NULL COMMENT '成交量(手)',
    amount      DECIMAL(20,4)  NULL COMMENT '成交额(千元)',
    PRIMARY KEY (ts_code, trade_date),
    KEY idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票日线行情';

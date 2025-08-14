-- 更新表注释
ALTER TABLE stock_factor COMMENT '股票技术因子表，包含MACD、KDJ、RSI、BOLL等技术指标';

-- 更新字段注释
ALTER TABLE stock_factor
    MODIFY COLUMN ts_code VARCHAR(10) NOT NULL COMMENT '股票代码（格式：000001.SZ）',
    MODIFY COLUMN trade_date DATE NOT NULL COMMENT '交易日期',
    MODIFY COLUMN close DECIMAL(10,4) COMMENT '当日收盘价',
    MODIFY COLUMN open DECIMAL(10,4) COMMENT '当日开盘价',
    MODIFY COLUMN high DECIMAL(10,4) COMMENT '当日最高价',
    MODIFY COLUMN low DECIMAL(10,4) COMMENT '当日最低价',
    MODIFY COLUMN pre_close DECIMAL(10,4) COMMENT '昨日收盘价',
    MODIFY COLUMN `change` DECIMAL(10,4) COMMENT '涨跌额（当日收盘价-昨日收盘价）',
    MODIFY COLUMN pct_change DECIMAL(10,4) COMMENT '涨跌幅（未复权，单位：%）',
    MODIFY COLUMN vol DECIMAL(18,2) COMMENT '成交量（手）',
    MODIFY COLUMN amount DECIMAL(18,2) COMMENT '成交额（千元）',
    MODIFY COLUMN adj_factor DECIMAL(10,4) COMMENT '复权因子',
    MODIFY COLUMN open_hfq DOUBLE COMMENT '开盘价（后复权）',
    MODIFY COLUMN open_qfq DOUBLE COMMENT '开盘价（前复权）',
    MODIFY COLUMN close_hfq DOUBLE COMMENT '收盘价（后复权）',
    MODIFY COLUMN close_qfq DOUBLE COMMENT '收盘价（前复权）',
    MODIFY COLUMN high_hfq DOUBLE COMMENT '最高价（后复权）',
    MODIFY COLUMN high_qfq DOUBLE COMMENT '最高价（前复权）',
    MODIFY COLUMN low_hfq DOUBLE COMMENT '最低价（后复权）',
    MODIFY COLUMN low_qfq DOUBLE COMMENT '最低价（前复权）',
    MODIFY COLUMN pre_close_hfq DOUBLE COMMENT '昨收价（后复权）',
    MODIFY COLUMN pre_close_qfq DOUBLE COMMENT '昨收价（前复权）',
    MODIFY COLUMN macd_dif DECIMAL(10,4) COMMENT 'MACD的DIF值（基于前复权价格计算）',
    MODIFY COLUMN macd_dea DECIMAL(10,4) COMMENT 'MACD的DEA值（也称MACD值，基于前复权价格计算）',
    MODIFY COLUMN macd DECIMAL(10,4) COMMENT 'MACD指标，即MACD柱（基于前复权价格计算）',
    MODIFY COLUMN kdj_k DECIMAL(10,4) COMMENT 'KDJ的K值，默认参数9,3,3',
    MODIFY COLUMN kdj_d DECIMAL(10,4) COMMENT 'KDJ的D值，默认参数9,3,3',
    MODIFY COLUMN kdj_j DECIMAL(10,4) COMMENT 'KDJ的J值，默认参数9,3,3',
    MODIFY COLUMN rsi_6 DECIMAL(10,4) COMMENT 'RSI指标，周期为6日',
    MODIFY COLUMN rsi_12 DECIMAL(10,4) COMMENT 'RSI指标，周期为12日',
    MODIFY COLUMN rsi_24 DECIMAL(10,4) COMMENT 'RSI指标，周期为24日',
    MODIFY COLUMN boll_upper DECIMAL(10,4) COMMENT '布林线上轨，默认参数20,2',
    MODIFY COLUMN boll_mid DECIMAL(10,4) COMMENT '布林线中轨，即20日移动平均线',
    MODIFY COLUMN boll_lower DECIMAL(10,4) COMMENT '布林线下轨，默认参数20,2',
    MODIFY COLUMN cci DOUBLE COMMENT 'CCI指标（顺势指标），默认参数14日';

-- 更新索引注释（MySQL 8.0及以上版本支持）
ALTER TABLE stock_factor
    DROP INDEX uk_code_date,
    ADD UNIQUE INDEX uk_code_date (ts_code, trade_date) COMMENT '股票代码和交易日期的唯一索引',
    DROP INDEX idx_trade_date,
    ADD INDEX idx_trade_date (trade_date) COMMENT '交易日期索引',
    DROP INDEX idx_ts_code,
    ADD INDEX idx_ts_code (ts_code) COMMENT '股票代码索引'; 
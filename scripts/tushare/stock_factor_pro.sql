CREATE TABLE `stock_factor_pro` (
  `ts_code` varchar(10) NOT NULL COMMENT '股票代码',
  `trade_date` date NOT NULL COMMENT '交易日期',
  `open` decimal(10,4) DEFAULT NULL COMMENT '开盘价',
  `high` decimal(10,4) DEFAULT NULL COMMENT '最高价',
  `low` decimal(10,4) DEFAULT NULL COMMENT '最低价',
  `close` decimal(10,4) DEFAULT NULL COMMENT '收盘价',
  `pre_close` decimal(10,4) DEFAULT NULL COMMENT '昨收价',
  `change` decimal(10,4) DEFAULT NULL COMMENT '涨跌额',
  `pct_chg` decimal(10,4) DEFAULT NULL COMMENT '涨跌幅',
  `vol` decimal(20,4) DEFAULT NULL COMMENT '成交量（手）',
  `amount` decimal(20,4) DEFAULT NULL COMMENT '成交额（千元）',
  `turnover_rate` decimal(10,4) DEFAULT NULL COMMENT '换手率（%）',
  `turnover_rate_f` decimal(10,4) DEFAULT NULL COMMENT '换手率（自由流通股）',
  `volume_ratio` decimal(10,4) DEFAULT NULL COMMENT '量比',
  `pe` decimal(10,4) DEFAULT NULL COMMENT '市盈率',
  `pe_ttm` decimal(10,4) DEFAULT NULL COMMENT '市盈率TTM',
  `pb` decimal(10,4) DEFAULT NULL COMMENT '市净率',
  `ps` decimal(10,4) DEFAULT NULL COMMENT '市销率',
  `ps_ttm` decimal(10,4) DEFAULT NULL COMMENT '市销率（TTM）',
  `dv_ratio` decimal(10,4) DEFAULT NULL COMMENT '股息率（%）',
  `dv_ttm` decimal(10,4) DEFAULT NULL COMMENT '股息率（TTM）（%）',
  `total_share` decimal(20,4) DEFAULT NULL COMMENT '总股本（万股）',
  `float_share` decimal(20,4) DEFAULT NULL COMMENT '流通股本（万股）',
  `free_share` decimal(20,4) DEFAULT NULL COMMENT '自由流通股本（万）',
  `total_mv` decimal(20,4) DEFAULT NULL COMMENT '总市值（万元）',
  `circ_mv` decimal(20,4) DEFAULT NULL COMMENT '流通市值（万元）',
  `adj_factor` decimal(10,4) DEFAULT NULL COMMENT '复权因子',
  
  -- 趋势指标
  `ma_5` decimal(10,4) DEFAULT NULL COMMENT '5日均线',
  `ma_10` decimal(10,4) DEFAULT NULL COMMENT '10日均线',
  `ma_20` decimal(10,4) DEFAULT NULL COMMENT '20日均线',
  `ma_30` decimal(10,4) DEFAULT NULL COMMENT '30日均线',
  `ma_60` decimal(10,4) DEFAULT NULL COMMENT '60日均线',
  `ma_90` decimal(10,4) DEFAULT NULL COMMENT '90日均线',
  `ma_250` decimal(10,4) DEFAULT NULL COMMENT '250日均线',
  
  -- MACD指标
  `macd` decimal(10,4) DEFAULT NULL COMMENT 'MACD指标',
  `macd_dif` decimal(10,4) DEFAULT NULL COMMENT 'MACD DIF值',
  `macd_dea` decimal(10,4) DEFAULT NULL COMMENT 'MACD DEA值',
  
  -- KDJ指标
  `kdj_k` decimal(10,4) DEFAULT NULL COMMENT 'KDJ K值',
  `kdj_d` decimal(10,4) DEFAULT NULL COMMENT 'KDJ D值',
  `kdj_j` decimal(10,4) DEFAULT NULL COMMENT 'KDJ J值',
  
  -- RSI指标
  `rsi_6` decimal(10,4) DEFAULT NULL COMMENT 'RSI-6值',
  `rsi_12` decimal(10,4) DEFAULT NULL COMMENT 'RSI-12值',
  `rsi_24` decimal(10,4) DEFAULT NULL COMMENT 'RSI-24值',
  
  -- BOLL指标
  `boll_upper` decimal(10,4) DEFAULT NULL COMMENT 'BOLL上轨',
  `boll_mid` decimal(10,4) DEFAULT NULL COMMENT 'BOLL中轨',
  `boll_lower` decimal(10,4) DEFAULT NULL COMMENT 'BOLL下轨',
  
  -- DMI指标
  `dmi_pdi` decimal(10,4) DEFAULT NULL COMMENT 'DMI上升动向值',
  `dmi_mdi` decimal(10,4) DEFAULT NULL COMMENT 'DMI下降动向值',
  `dmi_adx` decimal(10,4) DEFAULT NULL COMMENT 'DMI平均动向值',
  `dmi_adxr` decimal(10,4) DEFAULT NULL COMMENT 'DMI评估动向值',
  
  -- CCI指标
  `cci` decimal(10,4) DEFAULT NULL COMMENT 'CCI顺势指标',
  
  -- 其他技术指标
  `bias1` decimal(10,4) DEFAULT NULL COMMENT '6日BIAS',
  `bias2` decimal(10,4) DEFAULT NULL COMMENT '12日BIAS',
  `bias3` decimal(10,4) DEFAULT NULL COMMENT '24日BIAS',
  `vr` decimal(10,4) DEFAULT NULL COMMENT 'VR容量比率',
  `atr` decimal(10,4) DEFAULT NULL COMMENT '真实波动幅度均值',
  
  -- 连续涨跌统计
  `updays` int DEFAULT NULL COMMENT '连涨天数',
  `downdays` int DEFAULT NULL COMMENT '连跌天数',
  `topdays` int DEFAULT NULL COMMENT '近期最高价天数',
  `lowdays` int DEFAULT NULL COMMENT '近期最低价天数',
  
  PRIMARY KEY (`ts_code`,`trade_date`),
  KEY `idx_trade_date` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='股票技术指标数据'; 
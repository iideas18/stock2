-- PIT financials: first-announcement-date only (per spec).
CREATE TABLE IF NOT EXISTS cn_stock_fundamentals_pit (
    code              VARCHAR(10)   NOT NULL,
    report_period     DATE          NOT NULL,
    announcement_date DATE          NOT NULL,
    revenue           DECIMAL(20,4) NULL,
    net_profit        DECIMAL(20,4) NULL,
    total_assets      DECIMAL(20,4) NULL,
    total_equity      DECIMAL(20,4) NULL,
    operating_cf      DECIMAL(20,4) NULL,
    roe               DECIMAL(10,4) NULL,
    gross_margin      DECIMAL(10,4) NULL,
    net_margin        DECIMAL(10,4) NULL,
    pe                DECIMAL(10,4) NULL,
    pb                DECIMAL(10,4) NULL,
    ps                DECIMAL(10,4) NULL,
    PRIMARY KEY (code, report_period),
    INDEX idx_pit_ann (code, announcement_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS cn_stock_lhb_v2 (
    trade_date   DATE          NOT NULL,
    code         VARCHAR(10)   NOT NULL,
    seat         VARCHAR(128)  NOT NULL,
    buy_amount   DECIMAL(20,4) NOT NULL,
    sell_amount  DECIMAL(20,4) NOT NULL,
    reason       VARCHAR(256)  NULL,
    PRIMARY KEY (trade_date, code, seat),
    INDEX idx_lhb_code (code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS cn_stock_north_bound (
    trade_date   DATE          NOT NULL,
    code         VARCHAR(10)   NOT NULL,
    hold_shares  DECIMAL(24,4) NOT NULL,
    hold_ratio   DECIMAL(10,6) NULL,
    PRIMARY KEY (trade_date, code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS cn_stock_money_flow (
    trade_date       DATE          NOT NULL,
    code             VARCHAR(10)   NOT NULL,
    main_net_inflow  DECIMAL(24,4) NOT NULL,
    big_order_inflow DECIMAL(24,4) NOT NULL,
    PRIMARY KEY (trade_date, code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

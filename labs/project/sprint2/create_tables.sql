CREATE EXTERNAL TABLE IF NOT EXISTS gittba04.btcusdt (
    date TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE
)
PARTITIONED BY (year INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:/datos/gittba/gittba04/BTCUSDT';

ALTER TABLE gittba04.btcusdt SET TBLPROPERTIES ("skip.header.line.count"="1");
ALTER TABLE gittba04.btcusdt ADD PARTITION (year = 2021) LOCATION 'hdfs:/datos/gittba/gittba04/BTCUSDT/year=2021';
ALTER TABLE gittba04.btcusdt ADD PARTITION (year = 2022) LOCATION 'hdfs:/datos/gittba/gittba04/BTCUSDT/year=2022';
ALTER TABLE gittba04.btcusdt ADD PARTITION (year = 2023) LOCATION 'hdfs:/datos/gittba/gittba04/BTCUSDT/year=2023';
ALTER TABLE gittba04.btcusdt ADD PARTITION (year = 2024) LOCATION 'hdfs:/datos/gittba/gittba04/BTCUSDT/year=2024';
ALTER TABLE gittba04.btcusdt ADD PARTITION (year = 2025) LOCATION 'hdfs:/datos/gittba/gittba04/BTCUSDT/year=2025';
 
-- Repeat for all coins
CREATE EXTERNAL TABLE IF NOT EXISTS gittba04.ethusdt LIKE gittba04.btcusdt LOCATION 'hdfs:/datos/gittba/gittba04/ETHUSDT';
CREATE EXTERNAL TABLE IF NOT EXISTS gittba04.xrpusdt LIKE gittba04.btcusdt LOCATION 'hdfs:/datos/gittba/gittba04/XRPUSDT';
CREATE EXTERNAL TABLE IF NOT EXISTS gittba04.solusdt LIKE gittba04.btcusdt LOCATION 'hdfs:/datos/gittba/gittba04/SOLUSDT';
CREATE EXTERNAL TABLE IF NOT EXISTS gittba04.dogeusdt LIKE gittba04.btcusdt LOCATION 'hdfs:/datos/gittba/gittba04/DOGEUSDT';
CREATE EXTERNAL TABLE IF NOT EXISTS gittba04.adausdt LIKE gittba04.btcusdt LOCATION 'hdfs:/datos/gittba/gittba04/ADAUSDT';
CREATE EXTERNAL TABLE IF NOT EXISTS gittba04.shibusdt LIKE gittba04.btcusdt LOCATION 'hdfs:/datos/gittba/gittba04/SHIBUSDT';
CREATE EXTERNAL TABLE IF NOT EXISTS gittba04.dotusdt LIKE gittba04.btcusdt LOCATION 'hdfs:/datos/gittba/gittba04/DOTUSDT';
CREATE EXTERNAL TABLE IF NOT EXISTS gittba04.aaveusdt LIKE gittba04.btcusdt LOCATION 'hdfs:/datos/gittba/gittba04/AAVEUSDT';
CREATE EXTERNAL TABLE IF NOT EXISTS gittba04.xlmusdt LIKE gittba04.btcusdt LOCATION 'hdfs:/datos/gittba/gittba04/XLMUSDT';

-- Set TBLPROPERTIES for all tables
ALTER TABLE gittba04.ethusdt SET TBLPROPERTIES ("skip.header.line.count" = "1");
ALTER TABLE gittba04.xrpusdt SET TBLPROPERTIES ("skip.header.line.count" = "1");
ALTER TABLE gittba04.solusdt SET TBLPROPERTIES ("skip.header.line.count" = "1");
ALTER TABLE gittba04.dogeusdt SET TBLPROPERTIES ("skip.header.line.count" = "1");
ALTER TABLE gittba04.adausdt SET TBLPROPERTIES ("skip.header.line.count" = "1");
ALTER TABLE gittba04.shibusdt SET TBLPROPERTIES ("skip.header.line.count" = "1");
ALTER TABLE gittba04.dotusdt SET TBLPROPERTIES ("skip.header.line.count" = "1");
ALTER TABLE gittba04.aaveusdt SET TBLPROPERTIES ("skip.header.line.count" = "1");
ALTER TABLE gittba04.xlmusdt SET TBLPROPERTIES ("skip.header.line.count" = "1");

-- Add partitions for all coins
ALTER TABLE gittba04.ethusdt ADD PARTITION (year = 2021) LOCATION 'hdfs:/datos/gittba/gittba04/ETHUSDT/year=2021';
ALTER TABLE gittba04.ethusdt ADD PARTITION (year = 2022) LOCATION 'hdfs:/datos/gittba/gittba04/ETHUSDT/year=2022';
ALTER TABLE gittba04.ethusdt ADD PARTITION (year = 2023) LOCATION 'hdfs:/datos/gittba/gittba04/ETHUSDT/year=2023';
ALTER TABLE gittba04.ethusdt ADD PARTITION (year = 2024) LOCATION 'hdfs:/datos/gittba/gittba04/ETHUSDT/year=2024';
ALTER TABLE gittba04.ethusdt ADD PARTITION (year = 2025) LOCATION 'hdfs:/datos/gittba/gittba04/ETHUSDT/year=2025';
 year=
ALTER TABLE gittba04.xrpusdt ADD PARTITION (year = 2021) LOCATION 'hdfs:/datos/gittba/gittba04/XRPUSDT/year=2021';
ALTER TABLE gittba04.xrpusdt ADD PARTITION (year = 2022) LOCATION 'hdfs:/datos/gittba/gittba04/XRPUSDT/year=2022';
ALTER TABLE gittba04.xrpusdt ADD PARTITION (year = 2023) LOCATION 'hdfs:/datos/gittba/gittba04/XRPUSDT/year=2023';
ALTER TABLE gittba04.xrpusdt ADD PARTITION (year = 2024) LOCATION 'hdfs:/datos/gittba/gittba04/XRPUSDT/year=2024';
ALTER TABLE gittba04.xrpusdt ADD PARTITION (year = 2025) LOCATION 'hdfs:/datos/gittba/gittba04/XRPUSDT/year=2025';
 year=
ALTER TABLE gittba04.solusdt ADD PARTITION (year = 2021) LOCATION 'hdfs:/datos/gittba/gittba04/SOLUSDT/year=2021';
ALTER TABLE gittba04.solusdt ADD PARTITION (year = 2022) LOCATION 'hdfs:/datos/gittba/gittba04/SOLUSDT/year=2022';
ALTER TABLE gittba04.solusdt ADD PARTITION (year = 2023) LOCATION 'hdfs:/datos/gittba/gittba04/SOLUSDT/year=2023';
ALTER TABLE gittba04.solusdt ADD PARTITION (year = 2024) LOCATION 'hdfs:/datos/gittba/gittba04/SOLUSDT/year=2024';
ALTER TABLE gittba04.solusdt ADD PARTITION (year = 2025) LOCATION 'hdfs:/datos/gittba/gittba04/SOLUSDT/year=2025';
 
ALTER TABLE gittba04.dogeusdt ADD PARTITION (year = 2021) LOCATION 'hdfs:/datos/gittba/gittba04/DOGEUSDT/year=2021';
ALTER TABLE gittba04.dogeusdt ADD PARTITION (year = 2022) LOCATION 'hdfs:/datos/gittba/gittba04/DOGEUSDT/year=2022';
ALTER TABLE gittba04.dogeusdt ADD PARTITION (year = 2023) LOCATION 'hdfs:/datos/gittba/gittba04/DOGEUSDT/year=2023';
ALTER TABLE gittba04.dogeusdt ADD PARTITION (year = 2024) LOCATION 'hdfs:/datos/gittba/gittba04/DOGEUSDT/year=2024';
ALTER TABLE gittba04.dogeusdt ADD PARTITION (year = 2025) LOCATION 'hdfs:/datos/gittba/gittba04/DOGEUSDT/year=2025';
 
ALTER TABLE gittba04.adausdt ADD PARTITION (year = 2021) LOCATION 'hdfs:/datos/gittba/gittba04/ADAUSDT/year=2021';
ALTER TABLE gittba04.adausdt ADD PARTITION (year = 2022) LOCATION 'hdfs:/datos/gittba/gittba04/ADAUSDT/year=2022';
ALTER TABLE gittba04.adausdt ADD PARTITION (year = 2023) LOCATION 'hdfs:/datos/gittba/gittba04/ADAUSDT/year=2023';
ALTER TABLE gittba04.adausdt ADD PARTITION (year = 2024) LOCATION 'hdfs:/datos/gittba/gittba04/ADAUSDT/year=2024';
ALTER TABLE gittba04.adausdt ADD PARTITION (year = 2025) LOCATION 'hdfs:/datos/gittba/gittba04/ADAUSDT/year=2025';
 
ALTER TABLE gittba04.shibusdt ADD PARTITION (year = 2021) LOCATION 'hdfs:/datos/gittba/gittba04/SHIBUSDT/year=2021';
ALTER TABLE gittba04.shibusdt ADD PARTITION (year = 2022) LOCATION 'hdfs:/datos/gittba/gittba04/SHIBUSDT/year=2022';
ALTER TABLE gittba04.shibusdt ADD PARTITION (year = 2023) LOCATION 'hdfs:/datos/gittba/gittba04/SHIBUSDT/year=2023';
ALTER TABLE gittba04.shibusdt ADD PARTITION (year = 2024) LOCATION 'hdfs:/datos/gittba/gittba04/SHIBUSDT/year=2024';
ALTER TABLE gittba04.shibusdt ADD PARTITION (year = 2025) LOCATION 'hdfs:/datos/gittba/gittba04/SHIBUSDT/year=2025';
 
ALTER TABLE gittba04.dotusdt ADD PARTITION (year = 2021) LOCATION 'hdfs:/datos/gittba/gittba04/DOTUSDT/year=2021';
ALTER TABLE gittba04.dotusdt ADD PARTITION (year = 2022) LOCATION 'hdfs:/datos/gittba/gittba04/DOTUSDT/year=2022';
ALTER TABLE gittba04.dotusdt ADD PARTITION (year = 2023) LOCATION 'hdfs:/datos/gittba/gittba04/DOTUSDT/year=2023';
ALTER TABLE gittba04.dotusdt ADD PARTITION (year = 2024) LOCATION 'hdfs:/datos/gittba/gittba04/DOTUSDT/year=2024';
ALTER TABLE gittba04.dotusdt ADD PARTITION (year = 2025) LOCATION 'hdfs:/datos/gittba/gittba04/DOTUSDT/year=2025';
 
ALTER TABLE gittba04.aaveusdt ADD PARTITION (year = 2021) LOCATION 'hdfs:/datos/gittba/gittba04/AAVEUSDT/year=2021';
ALTER TABLE gittba04.aaveusdt ADD PARTITION (year = 2022) LOCATION 'hdfs:/datos/gittba/gittba04/AAVEUSDT/year=2022';
ALTER TABLE gittba04.aaveusdt ADD PARTITION (year = 2023) LOCATION 'hdfs:/datos/gittba/gittba04/AAVEUSDT/year=2023';
ALTER TABLE gittba04.aaveusdt ADD PARTITION (year = 2024) LOCATION 'hdfs:/datos/gittba/gittba04/AAVEUSDT/year=2024';
ALTER TABLE gittba04.aaveusdt ADD PARTITION (year = 2025) LOCATION 'hdfs:/datos/gittba/gittba04/AAVEUSDT/year=2025';
 
ALTER TABLE gittba04.xlmusdt ADD PARTITION (year = 2021) LOCATION 'hdfs:/datos/gittba/gittba04/XLMUSDT/year=2021';
ALTER TABLE gittba04.xlmusdt ADD PARTITION (year = 2022) LOCATION 'hdfs:/datos/gittba/gittba04/XLMUSDT/year=2022';
ALTER TABLE gittba04.xlmusdt ADD PARTITION (year = 2023) LOCATION 'hdfs:/datos/gittba/gittba04/XLMUSDT/year=2023';
ALTER TABLE gittba04.xlmusdt ADD PARTITION (year = 2024) LOCATION 'hdfs:/datos/gittba/gittba04/XLMUSDT/year=2024';
ALTER TABLE gittba04.xlmusdt ADD PARTITION (year = 2025) LOCATION 'hdfs:/datos/gittba/gittba04/XLMUSDT/year=2025';

-- Check Partitions for all tables
SHOW PARTITIONS gittba04.btcusdt;
SELECT * FROM gittba04.btcusdt WHERE year = 2023 LIMIT 10;
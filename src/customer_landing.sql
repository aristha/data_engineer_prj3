CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_landing` (
  `customerName` string,
  `email` string,
  `phone` string,
  `birthDay` date,
  `serialNumber` string,
  `registrationDate` bigint,
  `lastUpdateDate` bigint,
  `shareWithResearchAsOfDate` bigint,
  `shareWithPublicAsOfDate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://tamhv2-bucket/customer/'
TBLPROPERTIES ('classification' = 'json');
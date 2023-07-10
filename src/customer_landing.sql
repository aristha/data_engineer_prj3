CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_landing` (
  `customerName` string,
  `email` string,
  `phone` string,
  `birthDay` date,
  `serialNumber` string,
  `registrationDate` timestamp,
  `lastUpdateDate` timestamp,
  `shareWithResearchAsOfDate` timestamp,
  `shareWithPublicAsOfDate` timestamp
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://tam-p3/customer/'
TBLPROPERTIES ('classification' = 'json');
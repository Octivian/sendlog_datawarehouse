# sendlog_datawarehouse
hive for send_log format

make a jar,add it to hive classpath.

CREATE EXTERNAL TABLE test_send_log (
	userip STRING,
	serverip STRING,
	TIMESTAMP TIMESTAMP,
	uri STRING,
	referer STRING,
	HOST STRING,
	useragent STRING,
	cookie STRING,
	id INT
) PARTITIONED BY (dq INT, date DATE) 
	ROW FORMAT SERDE 'datawarehouse.io.serde.SendLogSerde' 
	STORED AS INPUTFORMAT 'datawarehouse.io.inputformat.SendLogInputFormat' 
	OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' 
	LOCATION '/user/kodo/test/hive/';

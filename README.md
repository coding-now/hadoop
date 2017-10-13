# hadoop操作示例代码
## excel数据解析导入hadoop
 支持.xls,.xlsx数据解析

## 基金元数据导入

  数据解析、存入hadoop
  ```
  hadoop jar hadoop-util-jar-with-dependencies.jar com.bigdata.hadoop.util.ExcelResolveDriver "hdfs://master.hadoop:9000/excel/funds.xls" "hdfs://master.hadoop:9000/funds/"
  ```

## 基金净值下载存入hadoop
### 历史数据获取
 ```
   hadoop jar hadoop-util-jar-with-dependencies.jar com.bigdata.hadoop.util.FundsStatDataRetriveDriver "hdfs://master.hadoop:9000/funds" "hdfs://master.hadoop:9000/data/funds-history"
 ```
### pig处理转换,存入hive
```
 env HADOOP_USER_NAME=hadoop

 fund = LOAD 'hdfs://master.hadoop:9000/data/funds-history' USING PigStorage(' ') AS (code,ts,c_pe,t_pe,ratio,b_s,s_s);
 --data_pre = filter fund by org.apache.pig.tutorial.NonURLDetector(code);
 data_list = filter fund by code matches '[0-9]*' ;
 data = filter data_list by c_pe >0;
 --dump data_list
 STORE data INTO 'hdfs://master.hadoop:9000/data/funds-clean2' USING PigStorage();

 CREATE TABLE fund_history(FUND_CODE varchar(10),TS varchar(15),C_PE FLOAT,TOTAL_PE FLOAT,INC_RATIO VARCHAR(7),BUY_S VARCHAR(20),SELL_S varchar(20),divi varchar(20))ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ';

 /user/hive/warehouse/fund_history

 load data inpath '/data/funds-clean2' overwrite into table default.fund_history;
 ```
## 选择排名靠前的基金
# hadoop
 excel数据解析，导入hadoop平台。
 .xls,.xlsx数据解析支持，优化。

hadoop jar excelReader-jar-with-dependencies.jar com.bigdata.hadoop.util.ExcelResolveDriver "hdfs://master.hadoop:9000/excel/funds.xls" "hdfs://master.hadoop:9000/data/funds"

 hadoop fs -mkdir -p /data/
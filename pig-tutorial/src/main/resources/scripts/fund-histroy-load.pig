-- 基金代码	基金简称	上市日期	当前规模(份)	基金管理人	基金发起人	基金托管人	最新基金净值
--codes = load 'hdfs://master.hadoop:9000/data/funds/part-m-00000' USING PigStorage('\t') AS (code,name,ipo,volumn,mgr,founder,agent,pe);
--code_list = foreach codes generate code;
--history = foreach code_list generate com.bigdata.pig.FundsRetrive(code);
--store history into '/data/funds-history-eps'  USING PigStorage();
--dump history;

fund =LOAD 'hdfs://172.17.6.114:9000/data/funds-history-bak' USING PigStorage(' ') AS (code,ts,c_pe,t_pe,ratio,b_s,s_s);
data = filter fund by org.apache.pig.tutorial.NonURLDetector(code);

STORE data INTO '/data/funds-clean' USING PigStorage();
--pig -Dpig.additional.jars=/home/hadoop/tutorial.jar pig-script/fund-history-load.pig

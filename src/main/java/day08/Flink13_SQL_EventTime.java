package day08;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink13_SQL_EventTime {
    public static void main(String[] args) {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将本地时区改为格林威治时间
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.local-time-zone", "GMT");


        //3.在创建表的时候指定事件时间
        tableEnv.executeSql(("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int, " +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second)" +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor-sql.txt',"
                + "'format' = 'csv'"
                + ")"
        ));

        tableEnv.executeSql("select * from sensor").print();
    }
}

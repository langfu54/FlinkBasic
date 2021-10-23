package day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink03_SQL_GroupWindow_Hop {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int, " +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second)" +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor-sql.txt',"
                + "'format' = 'csv'"
                + ")");

        //开启滑动窗口
        tableEnv.executeSql("select\n" +
                " id, \n" +
                " sum(vc), \n" +
                " Hop_Start(t, interval '1' second ,interval '2' second), \n" +
                " Hop_End(t, interval '1' second ,interval '2' second) \n" +
                " from sensor \n" +
                " group by Hop(t, interval '1' second ,interval '2' second), id").print();

    }
}

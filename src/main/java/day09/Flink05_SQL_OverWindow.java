package day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink05_SQL_OverWindow {
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

        //利用sql做开窗操作
//        tableEnv.executeSql("select\n" +
//                " id, \n" +
//                " t, \n" +
//                " vc,\n" +
//                " sum(vc) over(partition by id order by t) \n" +
//                " from sensor").print();
        tableEnv.executeSql(  "select " +
                "id," +
                "vc," +
                "count(vc) over w, " +
                "sum(vc) over w " +
                "from sensor " +
                "window w as (partition by id order by t rows between 1 PRECEDING and current row)"
        ).print();

    }
}

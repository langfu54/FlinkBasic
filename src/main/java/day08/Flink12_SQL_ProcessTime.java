package day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink12_SQL_ProcessTime {
    public static void main(String[] args) {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.在创建表的时候指定处理时间
        tableEnv.executeSql(("create table sensor(id string,ts bigint,vc int,pt as PROCTIME()) with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor.txt',"
                + "'format' = 'csv'"
                + ")"
        ));

        tableEnv.executeSql("select * from sensor").print();
    }
}

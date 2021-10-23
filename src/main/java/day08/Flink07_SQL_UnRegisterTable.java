package day08;

import bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink07_SQL_UnRegisterTable {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<WaterSensor> streamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        //3.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 4.将流转为未注册的动态表
        Table table = tableEnv.fromDataStream(streamSource);

        //5.写TableAPI查询出id等于sensor_1的数据
        //写法一：
        Table result = tableEnv.sqlQuery("select * from " + table+" where id='sensor_1'");
        result.execute().print();

//        tableEnv.executeSql("select * from " + table+" where id='sensor_1'").print();


        //6.将结果表转为流(追加流)
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(result, Row.class);
//
//        rowDataStream.print();
//
//        env.execute();

    }
}

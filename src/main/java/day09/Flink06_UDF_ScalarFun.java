package day09;

import bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink06_UDF_ScalarFun {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("senso", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_", 5000L, 50),
                new WaterSensor("sr_2", 6000L, 60));

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.将流转为动态表
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);

        //TODO a.不注册直接使用
//        table
//                .select(call(myUDF.class, $("id"))).execute().print();
        //TODO b.先注册再使用
        tableEnv.createTemporarySystemFunction("myFun", myUDF.class);

        //Table API
//        table
//                .select(call("myFun", $("id"))).execute().print();
        //SQL
        tableEnv.executeSql("select myFun(id) from "+table).print();
    }

    //计算id长度
    public static class myUDF extends ScalarFunction{
        public int eval(String value){
            return value.length();
        }
    }
}

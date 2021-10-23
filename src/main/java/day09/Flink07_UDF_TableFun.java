package day09;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink07_UDF_TableFun {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDataStreamSource = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.将流转为动态表
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);

        //TODO a.不注册直接使用
//        table
//                .joinLateral(call(MyUDTFFun.class,$("id")))
//                .select($("id"),$("word")).execute().print();
        //TODO b.先注册再使用
        tableEnv.createTemporarySystemFunction("myFun", MyUDTFFun.class);

        //Table API
//        table
//                .joinLateral(call("myFun", $("id")))
//                .select($("id"),$("word")).execute().print();
        //SQL
//        tableEnv.executeSql("select id,word from "+table+" ,lateral table(myFun(id))").print();
    }

    //自定义UDTF函数将id按照_切分出两个数据
    @FunctionHint(output = @DataTypeHint("ROW<word String>"))
    public static class MyUDTFFun extends TableFunction<Row>{

        public void eval(String value){
            String[] words = value.split("_");
            for (String word : words) {
                collect(Row.of(word));
            }
        }
    }

}

package day09;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink08_UDF_AggFun {
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
//                .groupBy($("id"))
//                .select($("id"),call(MyUDAFFun.class, $("vc"))).execute().print();
        //TODO b.先注册再使用
        tableEnv.createTemporarySystemFunction("myFun", MyUDAFFun.class);

        //Table API
//        table
//                .groupBy($("id"))
//                .select($("id"),call("myFun", $("vc"))).execute().print();
        //SQL
        tableEnv.executeSql("select id,myFun(vc) from "+table+" group by id").print();
    }

    //创建累加器
    public static class MyAcc {
        public Long sum = 0L;
        public Integer count = 0;
    }

    //自定义UDAF函数求vc的平均值
    public static class MyUDAFFun extends AggregateFunction<Double, MyAcc> {

        /**
         * 初始化累加器
         *
         * @param
         * @return
         */
        @Override
        public MyAcc createAccumulator() {
            return new MyAcc();
        }

        /**
         * 累加操作
         *
         * @param acc
         * @param value
         */
        public void accumulate(MyAcc acc, Integer value) {
            acc.sum += value;
            acc.count += 1;
        }

        /**
         * 得到最终结果
         *
         * @param accumulator
         * @return
         */
        @Override
        public Double getValue(MyAcc accumulator) {
            return accumulator.sum * 1D / accumulator.count;
        }


    }


}

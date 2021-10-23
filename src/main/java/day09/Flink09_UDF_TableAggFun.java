package day09;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.planner.expressions.Collect;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink09_UDF_TableAggFun {
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
//                .flatAggregate(call(MyUDTAFFun.class,$("vc")).as("value", "Rank"))
//                .select($("id"),$("value"),$("Rank")).execute().print();
        //TODO b.先注册再使用
        tableEnv.createTemporarySystemFunction("myFun", MyUDTAFFun.class);

        //Table API
        table
                .groupBy($("id"))
                .flatAggregate(call("myfun", $("vc")))
                .select($("id"),$("f0"),$("f1")).execute().print();
    }

    //创建累加器
    public static class MyTop2Acc {
        public Integer first;
        public Integer second;
    }

    //自定义UDATF函数求vc的Top2
    public static class MyUDTAFFun extends TableAggregateFunction<Tuple2<Integer,Integer>,MyTop2Acc>{

        /**
         * 初始化累加器
         * @return
         */
        @Override
        public MyTop2Acc createAccumulator() {
            MyTop2Acc acc = new MyTop2Acc();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            return acc;
        }

        /**
         * 累加操作
         * @param acc
         * @param value
         */
        public void accumulate(MyTop2Acc acc,Integer value){

            if (value>acc.first){
                //当前值大于第一个值，则先把第一个值变成第二，然后再把当前的值变成第一
                acc.second = acc.first;
                acc.first = value;
            }else if (value>acc.second){
                //当前值不大于第一，但是大于第二，则第一不变，把第二换成当前值
                acc.second = value;
            }

        }

        /**
         * 发送数据
         * @param acc
         * @param out
         */
        public void emitValue(MyTop2Acc acc, Collector<Tuple2<Integer,Integer>> out){

            if (acc.first!=Integer.MIN_VALUE){
                out.collect(Tuple2.of(acc.first,1));
            }

            if (acc.second!=Integer.MIN_VALUE){
                out.collect(Tuple2.of(acc.second,2));
            }
        }


    }

}

package day04;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Flink04_Project_UV {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<UserBehavior> userHehaviorDStream = streamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4])
                );
            }
        });

        //4.过滤出Pv的数据,并将数据转为Tuple
        SingleOutputStreamOperator<UserBehavior> uvToOneDStream = userHehaviorDStream.flatMap(new FlatMapFunction<UserBehavior, UserBehavior>() {
            @Override
            public void flatMap(UserBehavior value, Collector<UserBehavior> out) throws Exception {
                if ("pv".equals(value.getBehavior())) {
                    out.collect(value);
                }
            }
        });

        //5.按照pv分组
        KeyedStream<UserBehavior, Tuple> keyedStream = uvToOneDStream.keyBy("userId");

        //6.去重并统计个数
        keyedStream.process(new KeyedProcessFunction<Tuple, UserBehavior, Tuple2<String, Long>>() {
            //创建set集合用来去重 这个集合是每个分区创建一个
            HashSet<Long> uids = new HashSet<>();

            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                //将数据存入set集合
                uids.add(value.getUserId());

                out.collect(Tuple2.of("uv", (long) uids.size()));
            }
        }).print();

        env.execute();


    }
}

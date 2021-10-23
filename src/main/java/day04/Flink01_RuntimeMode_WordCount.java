package day04;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink01_RuntimeMode_WordCount {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 设置运行时模式
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //并行度设置为1
        env.setParallelism(1);

        //2.获取无界数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);
        //读取有界数据
//        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");

        //3.flatMap ->将数据按照空格切分并组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDStream = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        //4.将相同key的数据聚和到一块
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = wordToOneDStream.keyBy(0);

        //5.聚和计算 累加操作
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream.sum(1);
//        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Long>, Tuple2<String, Long>>() {
//            private ValueState<Long> valueState;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("lastSum", Long.class,0L));
//            }
//
//            @Override
//            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
//
//                Long lastSum = valueState.value();
//                lastSum += value.f1;
//                out.collect(Tuple2.of(value.f0, lastSum));
//
//                valueState.update(lastSum);
//            }
//        });

        result.print();

        env.execute();

    }
}

package day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink02_Stream_Bounded_WordCount {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //并行度设置为1
        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");

        //3.flatMap->将数据按照空格切出每一个单词
        SingleOutputStreamOperator<String> wordDStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //4.map -> 将单词组成Tuple元组
//        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDStream = wordDStream.map((MapFunction<String, Tuple2<String, Long>>) value -> Tuple2.of(value, 1L)).returns(Types.TUPLE(Types.STRING,Types.LONG));
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDStream = wordDStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return Tuple2.of(value, 1L);
            }
        });

        //5.将相同key的数据聚和到一块
//        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = wordToOneDStream.keyBy(0);
        //通过自定义key的选择器来决定key为哪一个
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordToOneDStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });

        //6.聚和计算
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream.sum(1);

        result.print();

        env.execute();
    }
}

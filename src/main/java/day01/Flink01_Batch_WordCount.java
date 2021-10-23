package day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_Batch_WordCount {
    public static void main(String[] args) throws Exception {
        //1.创建批执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.获取数据
        DataSource<String> dataSource = env.readTextFile("input/word.txt");

        //3.flatMap->将每一行数据按照空格切出每一个单词
        FlatMapOperator<String, String> word = dataSource.flatMap(new MyFlatMapFun());

        //4.map->将每一个单词组成Tuple元组
        MapOperator<String, Tuple2<String, Long>> wordToOne = word.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return Tuple2.of(value, 1L);
//                return new Tuple2<>(value, 1L);
            }
        });

        //5.reduceBykey -> 先将相同key的数据聚和到一块  ->再做聚合操作
        UnsortedGrouping<Tuple2<String, Long>> groupBy = wordToOne.groupBy(0);

        AggregateOperator<Tuple2<String, Long>> sum = groupBy.sum(1);

        //6.打印
        sum.print();
    }

    //自定义一个类实现FlatMapFunction接口
    public static class MyFlatMapFun implements FlatMapFunction<String,String>{

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                //通过采集器将数据发送到下游
                out.collect(word);
            }
        }
    }
}

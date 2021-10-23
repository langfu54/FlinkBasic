package day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_Transform_RichFun_Map {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据
//        DataStreamSource<Integer> dataStreamSource = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/word.txt");

        //3.TODO map 将数据+1返回
        SingleOutputStreamOperator<String> map = dataStreamSource.map(new MyMap());


        map.print();

        env.execute();
    }


    //富函数中的open方法和close方法，每个并行实例调用一次，并且open最先调用，close最后调用,但是在读文件时，每个并行实例close调用两次
    public static class MyMap extends RichMapFunction<String,String>{

        //默认生命周期方法，初始化一些东西，比如创建连接
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open...");
        }

        @Override
        public String map(String value) throws Exception {
            System.out.println(getRuntimeContext().getTaskNameWithSubtasks());
            return value+1;
        }

        //默认生命周期方法，清理一些东西，比如关闭连接
        @Override
        public void close() throws Exception {
            System.out.println("close...");
        }
    }
}

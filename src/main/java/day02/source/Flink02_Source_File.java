package day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Flink02_Source_File {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2.从文件中获取数据
//        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt").setParallelism(2);
        //从hdfs中读取文件
        DataStreamSource<String> streamSource = env.readTextFile("hdfs://hadoop102:8020/output2/123.txt").setParallelism(2);


        streamSource.print();

        env.execute();
    }
}

package day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_Transform_Map {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<Integer> dataStreamSource = env.fromElements(1, 2, 3, 4, 5);

        //3.TODO map 将数据+1返回
        SingleOutputStreamOperator<Integer> map = dataStreamSource.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                System.out.println("map...");
                return value + 1;
            }
        });


        map.print();

        env.execute();
    }
}

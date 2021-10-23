package day02.source;

import bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.SplittableIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Flink01_Source_Collcetion {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2.从Java集合中获取数据 并行度必须是1
        WaterSensor waterSensor = new WaterSensor("101", 1L, 1);
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(waterSensor);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);

        DataStreamSource<WaterSensor> streamSource = env.fromCollection(waterSensors);
//        env.fromParallelCollection(new SplittableIterator<String>() {
//        })

        //从元素中获取数据 这个并行必须是1
//        DataStreamSource<String> streamSource = env.fromElements("a", "b", "c", "d", "e").setParallelism(3);


        streamSource.print();

        env.execute();
    }
}

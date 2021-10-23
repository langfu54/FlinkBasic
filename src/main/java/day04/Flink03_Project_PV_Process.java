package day04;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink03_Project_PV_Process {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        //2.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //3.求总浏览量
        streamSource.process(new ProcessFunction<String, Tuple2<String, Long>>() {
            //创建累加器
            private Long count = 0L;

            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                //按照逗号切分
                String[] split = value.split(",");
                //判断是否是pv行为
                if ("pv".equals(split[3])) {
                    //是的话数据个数+1
                    count++;
                    out.collect(Tuple2.of("pv", count));
                }
            }
        }).print();

        env.execute();
    }
}

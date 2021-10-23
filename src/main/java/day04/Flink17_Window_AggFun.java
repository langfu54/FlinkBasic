package day04;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink17_Window_AggFun {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> flatMap = streamSource.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });

        //4.将相同的单词聚合到同一个分区
        KeyedStream<WaterSensor, Tuple> keyedStream = flatMap.keyBy("id");

        // 5.开启一个基于时间的滚动窗口
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(20)));

        //TODO 6.使用窗口函数 ->增量聚合函数 AggFun 输入输出的类型可以不一致
        window.aggregate(new AggregateFunction<WaterSensor, Long, Long>() {
            //创建累加器
            @Override
            public Long createAccumulator() {
                System.out.println("初始化累加器。。。");
                return 0L;
            }

            //累加操作
            @Override
            public Long add(WaterSensor value, Long accumulator) {
                System.out.println("累加操作。。。");
                return value.getVc() + accumulator;
            }

            //返回结果
            @Override
            public Long getResult(Long accumulator) {
                System.out.println("返回结果。。。");
                return accumulator;
            }

            //合并操作 通常在会话窗口中使用
            @Override
            public Long merge(Long a, Long b) {
                System.out.println("Merge。。。");
                return a + b;
            }
        }).print();


        env.execute();


    }
}

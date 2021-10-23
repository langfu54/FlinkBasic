package day04;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink20_Window_NoKeyWindow {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(4);

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

        //TODO 在keyBy之前使用 窗口是不分key的
        AllWindowedStream<WaterSensor, TimeWindow> windowAll = flatMap.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(20)));

        windowAll.process(new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                out.collect(elements.toString());
            }
        }).print();


//        KeyedStream<WaterSensor, String> keyedStream = flatMap.keyBy(r -> r.getId());

//        keyedStream.sum("vc").print();

        env.execute();


    }
}

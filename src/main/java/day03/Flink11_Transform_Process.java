package day03;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink11_Transform_Process {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

       //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorMap = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //TODO keyBy之前使用Process
//        SingleOutputStreamOperator<WaterSensor> processWhitFilter = waterSensorMap.process(new ProcessFunction<WaterSensor, WaterSensor>() {
//            @Override
//            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
//                if ("s1".equals(value.getId())) {
//                    out.collect(value);
//                }
//            }
//        });

        //3.对相同id的数据进行分组
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorMap.keyBy("id");

        //TODO keyBy之后使用 4.利用Process实现累加效果
        SingleOutputStreamOperator<WaterSensor> processWithSum = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {

            //定义一个变量用来保存之前累加的结果  有Bug->不会根据key来判断是保存的谁的值
            Integer lastSum = 0;

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                lastSum += value.getVc();
                out.collect(new WaterSensor(value.getId(), value.getTs(), lastSum));
            }
        });


        processWithSum.print();

        env.execute();
    }
}

package day05;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Flink06_WaterMark_Custom_Event {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //手动指定WaterMark生成时间
        env.getConfig().setAutoWatermarkInterval(5000);

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //TODO 4.自定义周期性生成的WaterMark
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDStream.assignTimestampsAndWatermarks(
                new WatermarkStrategy<WaterSensor>() {
                    @Override
                    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new MyPeriod(Duration.ofSeconds(2));
                    }
                }
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {

                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                }
                )
        );

        //5.KeyBy 按照id进行分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(r -> r.getId());

        //6.开窗 开启一个基于事件时间的滚动窗口
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        window.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                           @Override
                           public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                               String msg = "当前key: " + key
                                       + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                                       + elements.spliterator().estimateSize() + "条数据 ";
                               out.collect(msg);
                           }
                       }
        ).print();

        env.execute();
    }

    public static class MyPeriod implements WatermarkGenerator<WaterSensor> {

        private long maxTimestamp;

        private long outOfOrdernessMillis;

        public MyPeriod(Duration maxOutOfOrderness) {

            this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

            this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
        }

        /**
         * 在每个事件中调用，允许水印生成器检查并记住*事件时间戳，或根据事件本身发出水印
         *
         * @param event
         * @param eventTimestamp
         * @param output
         */
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            //与每个事件所携带的时间戳做对比，获取最大的时间戳

            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
            System.out.println("生成WaterMark:" + (maxTimestamp - outOfOrdernessMillis - 1));
            output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
        }

        /**
         * 周期性生成WaterMark默认200ms
         *
         * @param output
         */
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

        }
    }
}

package day07;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Flink10_CEP_Pattern_Group {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取文件中的数据
        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]) * 1000, Integer.parseInt(split[2]));
            }
        });
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2)
                        )
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );


        //TODO 4.定义模式
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .begin(Pattern.<WaterSensor>begin("begin")
                        .where(new IterativeCondition<WaterSensor>() {
                            @Override
                            public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                                return "sensor_1".equals(value.getId());
                            }
                        })
                        //严格连续
                        .next("next")
                        .where(new IterativeCondition<WaterSensor>() {
                            @Override
                            public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                                return "sensor_2".equals(value.getId());
                            }
                        })
                )
                .times(2);

        //TODO 5.将流作用于模式上
        PatternStream<WaterSensor> patternStream = CEP.pattern(waterSensorSingleOutputStreamOperator, pattern);

        //TODO 6.查询出模式匹配搭配到的数据
        patternStream.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                return pattern.toString();
            }
        }).print();

        env.execute();
    }
}

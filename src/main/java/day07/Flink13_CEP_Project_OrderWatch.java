package day07;

import bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Flink13_CEP_Project_OrderWatch {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件中获取数据->将数据转为JavaBean->指定WaterMark->按照订单ID分组
        KeyedStream<OrderEvent, Long> orderEventLongKeyedStream = env
                .readTextFile("input/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                                    @Override
                                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                        return element.getEventTime() * 1000;
                                    }
                                })
                )
                .keyBy(r -> r.getOrderId());

        //3.创建模式
//        统计创建订单到下单中间超过15分钟的超时数据以及正常的数据
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("start")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .next("next")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        //4.将流应用到模式上
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventLongKeyedStream, pattern);

        //5.获取符合要求数据
        SingleOutputStreamOperator<String> result = patternStream.select(new OutputTag<String>("output") {
                                                                         },
                new PatternTimeoutFunction<OrderEvent, String>() {

                    @Override
                    public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                        return pattern.toString();
                    }
                },
                new PatternSelectFunction<OrderEvent, String>() {

                    @Override
                    public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                        return pattern.toString();
                    }
                }
        );

        result.print("主流：");
        result.getSideOutput(new OutputTag<String>("output") {
        }).print("超时订单");

        env.execute();

    }

}

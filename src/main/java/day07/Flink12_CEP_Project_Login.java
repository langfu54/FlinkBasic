package day07;

import bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Flink12_CEP_Project_Login {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件读取数据并转为JavaBean，以及指定WaterMark
        SingleOutputStreamOperator<LoginEvent> loginEventSingleOutputStreamOperator = env
                .readTextFile("input/LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new LoginEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                                    @Override
                                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                        return element.getEventTime() * 1000;
                                    }
                                })
                );

        //按照相同用户的数据来进行分区所以要按照用户id进行分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventSingleOutputStreamOperator.keyBy(r -> r.getUserId());

        //3.创建模式
        //用户2秒内连续两次及以上登录失败则判定为恶意登录
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("start")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .timesOrMore(2)
                .consecutive()
                .within(Time.seconds(2));

        //4.将流作用到模式上
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);

        //5.获取匹配到的数据
        patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                return pattern.toString();
            }
        }).print();

        env.execute();

    }
}

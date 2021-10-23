package day08;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

public class Flink17_TableAPI_GroupWindow_CountSliding {
    public static void main(String[] args) {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 3.将流转为表,并指定事件时间

        //经过测试，处理时间只能指定为不存在的字段,但是事件时间可以
        Table table = tableEnv.fromDataStream(waterSensorStream, $("id"), $("ts"), $("vc"),$("pt").proctime());

        //4.开启一个基于元素个数的滑动窗口  必须指定处理时间字段
        Table result = table
//                .window(Tumble.over(rowInterval(3L)).on($("pt")).as("w"))
                .window(Slide.over(rowInterval(3L)).every(rowInterval(2L)).on($("pt")).as("w"))
                .groupBy($("id"), $("w"))
                .select($("id"), $("vc").sum().as("vcSum"));

        result.execute().print();

    }
}

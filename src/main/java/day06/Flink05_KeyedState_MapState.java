package day06;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.Int;

public class Flink05_KeyedState_MapState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //4.按照相同Id进行聚合
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDStream.keyBy(r -> r.getId());

        //5.去掉重复的水位值. 思路: 把水位值作为MapState的key来实现去重, value随意
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            //a.定义状态
            private MapState<Integer, WaterSensor> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //b.初始化状态
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, WaterSensor>("map-State", Integer.class, WaterSensor.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //c.使用状态
                //先将数据存入状态中
                if (!mapState.contains(value.getVc())){
                    mapState.put(value.getVc(), value);
                }

                //取出状态中的key
                WaterSensor result = mapState.get(value.getVc());
                out.collect(result.toString());
            }
        } ).print();

        env.execute();

    }

}

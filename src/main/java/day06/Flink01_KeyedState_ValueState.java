package day06;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink01_KeyedState_ValueState {
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

        //5.检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警。
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            //a.声明状态
            private ValueState<Integer> lastVc;


            @Override
            public void open(Configuration parameters) throws Exception {
                //b.初始化状态
                lastVc = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("valueState", Integer.class));

            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //c.使用状态,当是第一条数据的时候，状态里面是null，则把第一条数据作为初始值。
                Integer lastVcValue = lastVc.value() == null ? value.getVc() : lastVc.value();

                //判断当前水位与上一次水位差是否大于10
                if (Math.abs(value.getVc() - lastVcValue) > 10) {
                    out.collect("水位差超过10 报警！！！！");
                }

                //d.更新状态
                lastVc.update(value.getVc());

            }
        }).print();

        env.execute();

    }
}

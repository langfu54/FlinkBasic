package day06;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class Flink02_KeyedState_ListState {
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

        //5.针对每个传感器输出最高的3个水位值
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            private ListState<Integer> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("listState", Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //1.先将当前的值保存到状态中
                listState.add(value.getVc());

                //2.取出状态中的值
                ArrayList<Integer> vcS = new ArrayList<>();
                for (Integer vc : listState.get()) {
                    vcS.add(vc);
                }

                //3.对新的集合中的数据做排序
                vcS.sort((o1, o2) -> o2-o1);

                //4.删除集合中最后一条数据
                if (vcS.size()>3){
                    vcS.remove(3);
                }

                //5.将新的集合重新保存到状态中
                listState.update(vcS);

                out.collect(listState.get().toString());
            }
        } ).print();

        env.execute();

    }
}

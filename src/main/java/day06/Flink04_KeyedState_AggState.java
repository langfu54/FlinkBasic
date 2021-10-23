package day06;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink04_KeyedState_AggState {
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

        //5.计算每个传感器的平均水位
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            //a.定义状态
            private AggregatingState<Integer, Double> vcAvg;

            @Override
            public void open(Configuration parameters) throws Exception {
                //b.初始化状态
                vcAvg = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>("vcAvg", new MyAggFun(), Types.TUPLE(Types.INT, Types.INT)));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {

                //1.将数据保存到状态中做计算
                vcAvg.add(value.getVc());
                //2.取出状态中的值
                Double result = vcAvg.get();
                out.collect(value.getId()+":"+result);

            }
        } ).print();

        env.execute();

    }

    public static class MyAggFun implements AggregateFunction<Integer, Tuple2<Integer,Integer>, Double>{

        /**
         * 初始化累加器
         * Tuple中第一个值指的是vc的总和
         * Tuple中第二个值指的是元素个数
         * @return
         */
        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0,0);
        }

        /**
         * 累加操作
         * @param value
         * @param accumulator
         * @return
         */
        @Override
        public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
            return Tuple2.of(accumulator.f0+value, accumulator.f1+1);
        }

        /**
         * 获取计算结果
         * @param accumulator
         * @return
         */
        @Override
        public Double getResult(Tuple2<Integer, Integer> accumulator) {
            return accumulator.f0*1D/accumulator.f1;
        }

        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
            return Tuple2.of(a.f0+b.f0, a.f1+b.f1);
        }
    }
}

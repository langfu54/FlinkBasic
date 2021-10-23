package day06;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class Flink07_OpertaerStart_BroadcastState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> localSource = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> hadoopSource = env.socketTextStream("hadoop102", 9999);

        //3.初始化状态
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("state", String.class, String.class);

        //4.将流广播出去
        BroadcastStream<String> broadcast = localSource.broadcast(mapStateDescriptor);

        //5.将两条流连接
        BroadcastConnectedStream<String, String> connect = hadoopSource.connect(broadcast);

        //6.逻辑处理：通过广播的流来控制普通流的逻辑
        connect.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //获取到广播过来状态中的数据
                ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(mapStateDescriptor);
                //取出状态中的值,并根据不同的值做不同的处理
                if ("1".equals(state.get("swith"))) {
                    out.collect("执行逻辑1.。。。");
                } else if ("2".equals(state.get("swith"))) {
                    out.collect("执行逻辑2.。。。");
                } else {
                    out.collect("执行其他逻辑");
                }

            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {

                //1.首先在广播的流中提取到Map状态
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                //2.将数据放入状态中
                broadcastState.put("swith", value);
            }
        }).print();

        env.execute();
    }
}

package day03;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class Flink07_Transform_Connect {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6);
        DataStreamSource<String> stringDataStreamSource = env.fromElements("a", "b", "c", "d", "e", "f");

        //3.TODO Connect 将两条流连到一起 同床异梦
        ConnectedStreams<Integer, String> connect = integerDataStreamSource.connect(stringDataStreamSource);

        SingleOutputStreamOperator<String> result = connect.process(new CoProcessFunction<Integer, String, String>() {
            @Override
            public void processElement1(Integer value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value + "");
            }

            @Override
            public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value);
            }
        });

        result.print();

        env.execute();
    }
}

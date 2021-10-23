package day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_Source_Socket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 从socket中读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        streamSource.print();

        env.execute();
    }
}

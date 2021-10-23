package day02.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Flink04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        kafkaProperties.setProperty("group.id", "test");
        kafkaProperties.setProperty("auto.offset.reset", "latest");

        //TODO 从kafka获取数据
        DataStreamSource<String> streamSource = env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), kafkaProperties)).setParallelism(2);

        streamSource.print();

        env.execute();

    }
}

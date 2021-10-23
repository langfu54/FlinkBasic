package day03.sink;

import bean.WaterSensor;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class Flink01_Sink_kafka {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.对数据做处理
        SingleOutputStreamOperator<String> jsonStrDStream = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                //先将数据转为JavaBean
                String[] split = value.split(" ");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                String jsonString = JSON.toJSONString(waterSensor);
                return jsonString;
            }
        });

        //TODO 将数据发送至Kafka
        //配置kafka连接地址
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        //指定kafka的Topic以及序列化类型
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>("topic_sensor",new SimpleStringSchema(),properties);

        jsonStrDStream.addSink(kafkaProducer);

        env.execute();

    }
}

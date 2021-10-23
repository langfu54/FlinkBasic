package day03.sink;

import bean.WaterSensor;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Properties;

public class Flink02_Sink_Redis {
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

        //TODO 将数据发送至Redis

        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build();
        jsonStrDStream.addSink(new RedisSink<String>(jedisPoolConfig, new RedisMapper<String>() {
            //编写redis命令
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET,"0325");
            }

            //设置rediskey默认为rediskey 如果是Hash类型的话，则是数据中的小key
            @Override
            public String getKeyFromData(String s) {
                WaterSensor waterSensor = JSON.parseObject(s, WaterSensor.class);
                return waterSensor.getId();
            }

            //设置具体的value
            @Override
            public String getValueFromData(String s) {
                return s;
            }
        }));

        env.execute();

    }
}

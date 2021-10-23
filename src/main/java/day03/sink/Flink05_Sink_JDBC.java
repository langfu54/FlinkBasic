package day03.sink;

import bean.WaterSensor;
import com.mysql.jdbc.Driver;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Flink05_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.对数据做处理
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                //先将数据转为JavaBean
                String[] split = value.split(" ");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                return waterSensor;
            }
        });
        //自定义Sink 将数据写入 Mysql
        waterSensorDStream.addSink(JdbcSink.sink("insert into sensor values(?,?,?)", (ps,t)->{
           ps.setString(1,t.getId());
           ps.setLong(2, t.getTs());
           ps.setInt(3, t.getVc());
        },
                //WithBatchSize用来控制每次写入Mysql的条数
                //withBatchIntervalMs用来控制每次写入Mysql的时间
                // 以上两个参数任何一个达到阈值就写入
                new JdbcExecutionOptions.Builder().withBatchIntervalMs(10000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:mysql://hadoop102:3306/test?useSSL=false")
                .withUsername("root")
                .withPassword("000000")
                .withDriverName(Driver.class.getName())
                .build()
        ));

        env.execute();

    }

}

package day02.source;

import bean.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.Random;

public class Flink05_Source_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 自定义数据源
        DataStreamSource<WaterSensor> streamSource = env.addSource(new mySource()).setParallelism(2);

        streamSource.print();

        env.execute();

    }

    //自定义一个类实现SourceFunction这个接口来自定义数据源
    public static class mySource implements SourceFunction<WaterSensor>,ParallelSourceFunction<WaterSensor>

    {

        private Boolean isRunning = true;
        private Random random = new Random();

        //这个方法中是获取到数据发送出去的
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (isRunning){
                ctx.collect(new WaterSensor("sensor_"+ this.random.nextInt(100), System.currentTimeMillis(), this.random.nextInt(1000)));
                Thread.sleep(200);
            }
        }

        //这个方法时取消任务的，这个方法不是自己调的，系统内部
        @Override
        public void cancel() {
            isRunning = false;

        }
    }
}

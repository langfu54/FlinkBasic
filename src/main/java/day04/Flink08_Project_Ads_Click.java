package day04;

import bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Flink08_Project_Ads_Click {
    public static void main(String[] args) throws Exception {

        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.通过文件获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/AdClickLog.csv");


        //3.将数据转成KV->Tuple2元组，key->province adId value->1L
        SingleOutputStreamOperator<Tuple2<String, Long>> provinceWithAdIdToOneDStream = streamSource.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[2] + "-" + split[1], 1L);
            }
        });

        //4.将相同省份的广告分组
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = provinceWithAdIdToOneDStream.keyBy(0);

        //5.累加求和
        keyedStream.sum(1).print();

        env.execute();

    }
}

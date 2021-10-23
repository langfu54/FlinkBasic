package day04;

import bean.OrderEvent;
import bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Flink09_Project_Order {
    public static void main(String[] args) throws Exception {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        //2.分别从不同的数据源获取数据,并转为javaBean
        //获取订单相关的数据
        SingleOutputStreamOperator<OrderEvent> orderEventDStream = env.readTextFile("input/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new OrderEvent(
                                Long.parseLong(split[0]),
                                split[1],
                                split[2],
                                Long.parseLong(split[3])
                        );
                    }
                });

        //获取交易相关的数据
        SingleOutputStreamOperator<TxEvent> txEventDStream = env.readTextFile("input/ReceiptLog.csv")
                .map(new MapFunction<String, TxEvent>() {
                    @Override
                    public TxEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new TxEvent(
                                split[0],
                                split[1],
                                Long.parseLong(split[2])
                        );
                    }
                });

        //3.将两个流关联起来
        ConnectedStreams<OrderEvent, TxEvent> connect = orderEventDStream.connect(txEventDStream);

        ConnectedStreams<OrderEvent, TxEvent> keyBy = connect.keyBy("txId", "txId");

        //4.通过缓存的方式将两个流的数据关联起来
        keyBy.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
            //订单数据的缓存区
            HashMap<String, OrderEvent> orderMap = new HashMap<>();

            //交易数据的缓存区
            HashMap<String, TxEvent> txMap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                if (txMap.containsKey(value.getTxId())) {
                    //有相同的TxID证明能够关联上
                    out.collect("订单：" + value.getOrderId() + "对账成功");
                    //清除已经匹配上的数据
                    txMap.remove(value.getTxId());
                } else {
                    //没有关联上的,将自己存到缓存区
                    orderMap.put(value.getTxId(), value);
                }
            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                if (orderMap.containsKey(value.getTxId())) {
                    //有相同的TxID证明能够关联上
                    out.collect("订单：" + orderMap.get(value.getTxId()).getOrderId() + "对账成功");
                    //清除已经匹配上的数据
                    orderMap.remove(value.getTxId());
                } else {
                    //没有关联上的,将自己存到缓存区
                    txMap.put(value.getTxId(), value);
                }
            }
        }).print();

        env.execute();

    }


}

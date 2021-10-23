package day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink04_TableAPI_Source_Kafka {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

     /*   Schema schema = new Schema()
                .field("id", "String")
                .field("ts", "BigInt")
                .field("vc", "Integer"); */
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        //TODO 3.从Kafka中获取数据存放到临时表中
        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("sensor")
                .startFromLatest()
                .property("group.id", "bigdata")
                .property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
        )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //4.将临时表转为Table对象
        Table sensor = tableEnv.from("sensor");

        //5.查询临时表的数据，生成结果表
        Table resultTable = sensor
                .select($("id"), $("vc"), $("ts"));

        //6.将结果表转为流
        tableEnv.toAppendStream(resultTable, Row.class).print();

        env.execute();

    }
}

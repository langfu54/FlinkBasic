package day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Flink10_CataLog_With_Hive {
    public static void main(String[] args) {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //3.创建HiveCatalog
        String name            = "myhive";  // Catalog 名字
        String defaultDatabase = "flink_test"; // 默认数据库
        String hiveConfDir     = "c:/conf"; // hive配置文件的目录. 需要把hive-site.xml添加到该目录

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);

        //4.注册catalog
        tableEnv.registerCatalog(name, hive);

        //5.设置使用哪个catalog，以及使用哪个数据库
        tableEnv.useCatalog(name);
        tableEnv.useDatabase(defaultDatabase);

        //6.设置sql方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        //7.写sql查询hive中表的数据
        tableEnv.executeSql("select * from stu").print();


    }

}

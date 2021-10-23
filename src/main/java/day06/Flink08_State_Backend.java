package day06;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class Flink08_State_Backend {
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置状态后端为Memory
        env.setStateBackend(new MemoryStateBackend());

        //设置状态后端为FS
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck/0325"));

        //设置状态后端为RocksDB
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink/ck/RocksDB/0325"));

        //使用barrier不对齐
        env.getCheckpointConfig().enableUnalignedCheckpoints();

    }
}

package com.obes.apitest.state;

import com.obes.apitest.beans.SensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTest4_FaultTolerance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 状态后端配置
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend(""));
        env.setStateBackend(new RocksDBStateBackend(""));

        // 2. 检查点配置
        env.enableCheckpointing(300L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 检查点超时时间，防止数据阻塞
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 最大允许的同时checkpoint个数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 两个checkpoint之间最小的间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
        // true: 使用最近的checkpoint来恢复，不管savepoint是否更近
        // false: 最近的保存点恢复，不管是checkpoint或者是savepoint(default)
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        // 允许checkpoint最大失败次数，默认0
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);

        // 3. 重启策略配置
        // 固定延迟重启 参数：重启次数，两次重启的间隔时间
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        // 失败率重启 参数：重启次数，10分钟内最多重启3次，每两次重启的时间间隔
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));

        // 从文件读取路径
//        DataStream<String> inputStream = env.readTextFile("/Users/pengshuhai/data/java_project/FlinkDemo_2/src/main/resources/sensor.txt");

        // socket
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个flatmap操作，检测温度跳变，输出报警
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream.keyBy("id")
                .flatMap(new StateTest3_KeyedStateApplicationCase.TempChangeWarning(10.0));

        resultStream.print();

        env.execute();

    }
}

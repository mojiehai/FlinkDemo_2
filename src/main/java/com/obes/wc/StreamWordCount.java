package com.obes.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
//        env.setParallelism(2);
        // 设置不参与任务链的合并
//        env.disableOperatorChaining();

        // 从文件读取数据
//        String inputPath = "/Users/pengshuhai/data/java_project/FlinkDemo_2/src/main/resources/hello.txt";
//        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 用parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 从socket文本流读取数据
        // 本地起一个建议客户端命令：nc -lk 7777
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper()).slotSharingGroup("green")
                //.keyBy(0)
                .keyBy(v -> v.f0)
                .sum(1)
                .setParallelism(2).slotSharingGroup("red");  // 设置每一步并行度

        resultStream.print().setParallelism(1);

        // 执行任务
        env.execute();
    }

}

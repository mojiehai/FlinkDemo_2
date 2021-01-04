package com.obes.apitest.window;

import com.obes.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取路径
        //DataStream<String> inputStream = env.readTextFile("/Users/pengshuhai/data/java_project/FlinkDemo_2/src/main/resources/sensor.txt");

        // socket
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 开窗测试

        // 1. 增量聚合函数
        DataStream<Integer> resultStream = dataStream.keyBy(SensorReading::getId)
                //.countWindow(10, 2)    // 计数窗口(一个参数表示滚动窗口(窗口宽度),两个表示滑动窗口(第一个窗口宽度，第二个步长))
                //.window(EventTimeSessionWindows.withGap(Time.minutes(1)))  // 会话窗口
                .timeWindow(Time.seconds(15))  // 滚动窗口(一个参数表示滚动窗口(窗口宽度),两个表示滑动窗口(第一个窗口宽度，第二个步长))
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(15))) //滚动窗口
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    // 范型1 ： input
                    // 范型2 ： 累加器
                    // 范型3 ： output

                    // 该demo功能，统计窗口中数据个数

                    // 创建累加器 默认为0
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    // 一个数据来了后的操作，累加器+1
                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    // 窗口结束后，获取的内容，直接返回累加的数值
                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    // 主要用到session window里面涉及合并的操作，这里用不到，默认两个状态相加
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });


        // 2. 全窗口函数
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> resultStream2 = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                // process和apply差不多，但是内容更丰富
                //.process(new ProcessWindowFunction<SensorReading, Object, Tuple, TimeWindow>() {
                //})
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = tuple.getField(0);
                        Long windowEnd = window.getEnd();
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(id, windowEnd, count));
                    }
                });

        // 3. 其他可选api
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                //.trigger()
                //.evictor()
                .allowedLateness(Time.minutes(1))   // 允许数据迟到1分钟，窗口到时间了计算完了等1min再关闭，该窗口的数据1min内进入可以再次计算(延迟的数据，来一个算一次)
                .sideOutputLateData(outputTag)  // 延迟1分钟关闭，还迟到的数据，放入侧输出流
                .sum("temperature");

        // 获取之前迟到的放入侧输出流的数据
        sumStream.getSideOutput(outputTag).print("late");

        resultStream2.print();

        env.execute();
    }
}

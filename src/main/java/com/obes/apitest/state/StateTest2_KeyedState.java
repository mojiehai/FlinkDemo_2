package com.obes.apitest.state;

import com.obes.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTest2_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取路径
//        DataStream<String> inputStream = env.readTextFile("/Users/pengshuhai/data/java_project/FlinkDemo_2/src/main/resources/sensor.txt");

        // socket
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个有状态的map操作,统计当前sensor数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream
                .keyBy("id")
                .map(new MyKeyCountMapper());

        resultStream.print();

        env.execute();

    }

    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {

        private ValueState<Integer> keyCountState;

        // 其他类型状态的声明
        private ListState<String> myListState;
        private MapState<String, Double> myMapState;
        private ReducingState<SensorReading> myReducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            keyCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Integer>("key-count", Integer.class, 0));

            myListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<String>("my-list", String.class));

            myMapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));

            myReducingState = getRuntimeContext().getReducingState(
                    new ReducingStateDescriptor<SensorReading>("my-reducing", new ReduceFunction<SensorReading>() {
                        @Override
                        public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                            return new SensorReading(value1.getId(), value1.getTimestamp(), (value1.getTemperature() + value2.getTimestamp()) / 2);
                        }
                    }, SensorReading.class));
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            // 其他状态api调用
            // list state
            //Iterable<String> strings = myListState.get();
            for (String str: myListState.get()) {
                System.out.println(str);
            }
            myListState.add("hello");

            // map state
            myMapState.get("1");
            myMapState.put("2", 12.3);
            myMapState.remove("2");

            // reducing state
            myReducingState.add(value);
            myReducingState.clear();

            Integer count = keyCountState.value();
            count ++;
            keyCountState.update(count);
            return count;
        }
    }
}

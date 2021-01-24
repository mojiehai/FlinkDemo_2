package com.obes.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

public class TableTest4_KafkaPipeLine {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 连接kafka，读取数据
        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("sensor")
                .property("zookepper.connect", "localhost:2181")
                .property("bootstrap.servers", "kafka:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        // 3. 简单转换
        Table sensorTable = tableEnv.from("inputTable");
        // 简单转换
        Table resultTable = sensorTable.select("id, temp")
                .filter("id = 'sensor_6'");
        //.filter($("id").isEqual("sensor_6"));

        // 聚合统计
        Table aggTable = sensorTable.groupBy("id")
                .select("id, id.count as count, temp.avg as avgTemp");  // kafka也不支持回撤流,所有当前这个aggTable无法输出到kafka


        // 4. 建立kafka连接，输出到不同到topic
        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("sinktest")
                .property("zookepper.connect", "localhost:2181")
                .property("bootstrap.servers", "kafka:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        //.field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");

        //env.execute();
        tableEnv.execute("kafka");

    }
}

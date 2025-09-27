package org.apache.flink.streaming.examples.datagen;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DatagenExample {
/*    public static void main(String[] args) throws Exception {
        // 创建 Flink 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 定义数据模式
        DatagenSchema schema = new DatagenSchema()
                .field("id", Integer.class)
                .field("name", String.class)
                .field("timestamp", Long.class);

        // 创建 Datagen 数据源
        DataStream<Row> stream = env.addSource(new Datagen()
                .schema(schema)  // 使用自定义模式
                .rowsPerSecond(10));  // 每秒生成10行数据

        // 输出生成的流
        stream.print();

        // 执行任务
        env.execute("Flink Datagen Example");
    }*/
}

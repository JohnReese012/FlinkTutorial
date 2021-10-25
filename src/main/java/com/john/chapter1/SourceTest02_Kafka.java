package com.john.chapter1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class SourceTest02_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.3.192:9092");
//        properties.setProperty("group.id", "test");
        SingleOutputStreamOperator<Tuple2<String, Integer>> stream = env
                .addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties))
                .flatMap(new MyFlatMapper())
                .keyBy(0)
                .sum(1);

        stream.print();
        env.execute();

    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            collector.collect(new Tuple2<>(s, 1));
        }
    }
}

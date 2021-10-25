package com.john.chapter1;

import com.john.chapter1.beans.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SourceTest03_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> dataStream = env.readTextFile("C:\\Users\\xxw85\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\Sensor.txt");


        SingleOutputStreamOperator<SensorReading> resultDataStream = dataStream.flatMap(new MyFlatMapper());

        resultDataStream.print();
        env.execute();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, SensorReading> {

        @Override
        public void flatMap(String s, Collector<SensorReading> collector) {
            String[] strings = s.split(",");
            collector.collect(new SensorReading(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2])));
        }
    }

}

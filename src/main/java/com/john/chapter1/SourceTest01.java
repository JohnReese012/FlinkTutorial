package com.john.chapter1;

import com.john.chapter1.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceTest01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从集合中读取数据
        DataStreamSource<SensorReading> dataStream = env.fromCollection((Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        )));

        DataStreamSource<Integer> integerDataStream = env.fromElements(1, 2, 3, 4, 5);


        dataStream.print();
        integerDataStream.print();

        // 执行
        env.execute();
    }
}

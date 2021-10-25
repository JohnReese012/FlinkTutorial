package com.john;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

// 批处理word count
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件在读出数据
        String inputPath = "C:\\Users\\xxw85\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataSource<String> inputDataSet = env.readTextFile(inputPath);

        // 对数据集进行处理,按空格
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0) // 按照第一个位置的word分组
                .sum(1);//将第二个位置上的数据求和

       resultSet.print();

    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            // 按空格分词
            String[] words = s.split(" ");
            // 遍历所有words，包装成二元组
            for (String word : words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}

package com.iamhotcenplotz;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * DataStream word count demo using readFile (bounded stream)
 * @author Hotcenplotz
 * @version 1.0
 * @date 2026-05-04 11:34
 */
public class WordCountStream {
    public static void main(String[] args) throws Exception {
        // TODO 1. create execution enviroment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 2. stream data handling
        DataStreamSource<String> lineDS = env.readTextFile("input/words.txt");

        // TODO 3. output data
        SingleOutputStreamOperator<Tuple2<String, Integer>> splitData = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

                // slice line data
                for (String word : value.split(" ")) {
                    // send split data to downstream
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> ks = splitData.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = ks.sum(1); // 1 is position index

        result.print();

        // TODO 4. trigger execution
        env.execute();
    }
}

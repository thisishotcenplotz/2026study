package com.iamhotcenplotz;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * DataStream word count demo using socket(unbounded stream)
 * @author Hotcenplotz
 * @version 1.0
 * @date 2026-05-04 11:50
 */
public class WordCountStreamUnbounded {
    public static void main(String[] args) throws Exception {
        // TODO 1. create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 2. read data from socket
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop101", 7777);

        // TODO 3. data process: split transform group aggregate
        SingleOutputStreamOperator<Tuple2<String, Integer>> results = socketDS.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
            for (String word : value.split(" ")) {
                out.collect(Tuple2.of(word, 1));
            }
        })
        .returns(Types.TUPLE(Types.STRING, Types.INT))
        .keyBy(
        value -> value.f0
        )
        .sum(1);

        // TODO 4. result output
        results.print();

        // TODO 5. trigger execution
        env.execute();
    }
}

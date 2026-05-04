package com.iamhotcenplotz;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * Word Count Demo Using DataSet API （Not recommended）
 * */
public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        // TODO 1. create execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        // TODO 2. read file data
        DataSource<String> textFile = env.readTextFile("input/words.txt");

        // TODO 3. split data into tuple (word,1)
        FlatMapOperator<String, Tuple2<String, Integer>> wordsDataSet = textFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

                // split line by " "
                String[] words = value.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple = Tuple2.of(word, 1);

                    // call collection to output tuples
                    out.collect(wordTuple);
                }
            }
        });

        // TODO 4. aggregate words
        UnsortedGrouping<Tuple2<String, Integer>> groupedWords = wordsDataSet.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> results = groupedWords.sum(1); // 1 is position index
        // TODO 5. output result
        results.print();
    }
}

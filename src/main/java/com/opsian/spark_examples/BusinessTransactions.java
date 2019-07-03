package com.opsian.spark_examples;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BusinessTransactions
{
    public static void foo(String[] args)
    {
        final String transactionsPath = args[0];
        final String usersPath = args[1];
        final String outputPath = args[2];

        final SparkSession spark = SparkSession
            .builder()
            .appName("BusinessTransactions")
            .getOrCreate();

        final JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

        final List<Tuple2<String, String>> output = run(transactionsPath, usersPath, sparkContext);

        sparkContext
            .parallelizePairs(output)
            .saveAsHadoopFile(outputPath, String.class, String.class, TextOutputFormat.class);

        spark.stop();
    }

    public static List<Tuple2<String, String>> run(
        final String transactionsPath, final String usersPath, final JavaSparkContext sparkContext)
    {
        final JavaRDD<String> transactionInputFile = sparkContext.textFile(transactionsPath);
        JavaPairRDD<Integer, Integer> transactionPairs = transactionInputFile
            .filter(line -> !line.trim().isEmpty())
            .mapToPair(line ->
        {
            String[] transactionSplit = line.split("\\s+");
            return new Tuple2<>(
                Integer.valueOf(transactionSplit[2]),
                Integer.valueOf(transactionSplit[1]));
        });

        final JavaRDD<String> customerInputFile = sparkContext.textFile(usersPath);
        JavaPairRDD<Integer, String> customerPairs = customerInputFile.mapToPair(line ->
        {
            String[] customerSplit = line.split("\\s+");
            return new Tuple2<>(Integer.valueOf(customerSplit[0]), customerSplit[3]);
        });

        final Map<Integer, Long> result = transactionPairs
            .leftOuterJoin(customerPairs)
            .values()
            .distinct()
            .mapToPair(a -> new Tuple2<>(a._1, a._2.get()))
            .countByKey();

        return result
            .entrySet()
            .stream()
            .map(e -> new Tuple2<>(String.valueOf(e.getKey()), String.valueOf(e.getValue())))
            .collect(Collectors.toList());
    }
}

package com.opsian.spark_examples;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BusinessTransactions
{
    public static void main(String[] args)
    {
        final String thisDir = new File(".").getAbsolutePath() + "/";
        final String transactionsPath = thisDir + "big-transactions";
        final String usersPath = thisDir + "big-users";
        final String outputPath = thisDir + "results";

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
        JavaPairRDD<Integer, Integer> transactions = parseTransactionsFile(transactionsPath, sparkContext);
        JavaPairRDD<Integer, String> users = parseUsersFile(usersPath, sparkContext);

        final Map<Integer, Long> result = transactions
            .leftOuterJoin(users)
            .values()
            .distinct()
            .mapToPair(a -> new Tuple2<>(a._1, a._2.get()))
            .countByKey();

        return result
            .entrySet()
            .stream()
            .map(e -> new Tuple2<>(e.getKey().toString(), e.getValue().toString()))
            .collect(Collectors.toList());
    }

    private static JavaPairRDD<Integer, String> parseUsersFile(
        final String usersPath, final JavaSparkContext sparkContext)
    {
        final JavaRDD<String> userInputFile = sparkContext.textFile(usersPath);
        return userInputFile.mapToPair(line ->
        {
            String[] userSplit = line.split("\\s+");
            final String userId = userSplit[0];
            final String country = userSplit[3];
            return new Tuple2<>(Integer.parseInt(userId), country);
        });
    }

    private static JavaPairRDD<Integer, Integer> parseTransactionsFile(
        final String transactionsPath, final JavaSparkContext sparkContext)
    {
        final JavaRDD<String> transactionInputFile = sparkContext.textFile(transactionsPath);
        return transactionInputFile
            .filter(line -> !line.trim().isEmpty())
            .mapToPair(line ->
        {
            String[] transactionSplit = line.split("\\s+");
            final int quantity = Integer.parseInt(transactionSplit[1]);
            final int userId = Integer.parseInt(transactionSplit[2]);
            return new Tuple2<>(
                userId,
                quantity);
        });
    }
}

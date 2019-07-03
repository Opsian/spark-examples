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

public class BusinessTransactionsOptimized
{
    private final String transactionsPath;
    private final String usersPath;
    private final SparkSession spark;
    private final JavaSparkContext sparkContext;

    public BusinessTransactionsOptimized(
        final String transactionsPath, final String usersPath, final boolean isLocal)
    {
        this.transactionsPath = transactionsPath;
        this.usersPath = usersPath;

        final SparkSession.Builder builder = SparkSession.builder()
            .appName("BusinessTransactionsOptimized")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        if (isLocal)
        {
            builder.master("local");
        }

        spark = builder.getOrCreate();

        sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    }

    public static void main(String[] args)
    {
        final String thisDir = new File(".").getAbsolutePath() + "/";
        final String transactionsPath = thisDir + "big-transactions";
        final String usersPath = thisDir + "big-users";
        final String outputPath = thisDir + "results";
        final BusinessTransactionsOptimized job = new BusinessTransactionsOptimized(transactionsPath, usersPath, false);
        final List<Tuple2<String, String>> output = job.run();
        job.save(output, outputPath);
    }

    private void save(final List<Tuple2<String, String>> output, final String outputPath)
    {
        sparkContext
            .parallelizePairs(output)
            .saveAsHadoopFile(outputPath, String.class, String.class, TextOutputFormat.class);

        spark.stop();
    }

    public List<Tuple2<String, String>> run()
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
            int index = 0;

            // Skip column 0
            index = skipContent(line, index);

            // skip gap 0
            index = skipWhitespace(line, index);

            // get column 1
            int column1Start = index;
            index = skipContent(line, index);
            String column1 = line.substring(column1Start, index);

            // skip gap 1
            index = skipWhitespace(line, index);

            // get column 2
            int column2Start = index;
            index = skipContent(line, index);
            String column2 = line.substring(column2Start, index);

            final int quantity = Integer.parseInt(column1);
            final int userId = Integer.parseInt(column2);
            return new Tuple2<>(userId, quantity);
        });
    }

    private static int skipWhitespace(final String line, int index)
    {
        while (isWhiteSpace(line, index)) {
            index++;
        }

        return index;
    }

    private static int skipContent(final String line, int index)
    {
        while (!isWhiteSpace(line, index)) {
            index++;
        }

        return index;
    }

    private static boolean isWhiteSpace(final String line, final int index)
    {
        final char ch = line.charAt(index);
        return ch == ' ' || ch == '\t';
    }

}

package com.opsian.spark_examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class WordCount
{
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws InterruptedException
    {
        String file = "/home/richard/Projects/spark_examples/src/main/resources/example_document";

        SparkSession spark = SparkSession
            .builder()
            .appName("JavaWordCount")
            .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(file).javaRDD();
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> {
            logProcessInfo();
            return new Tuple2<>(s, 1);
        });
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> {
            logProcessInfo();

            return i1 + i2;
        });

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output)
        {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        logProcessInfo();

        spark.stop();
    }

    public static void logProcessInfo()
    {
        System.out.println(ManagementFactory.getRuntimeMXBean().getName());
        System.out.println(Thread.currentThread());
    }

}

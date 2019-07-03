package com.opsian.spark_examples;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BusinessTransactionsEmpty
{
    private final String transactionsPath;
    private final String usersPath;
    private final SparkSession spark;
    private final JavaSparkContext sparkContext;

    public BusinessTransactionsEmpty(
        final String transactionsPath, final String usersPath, final boolean isLocal)
    {
        this.transactionsPath = transactionsPath;
        this.usersPath = usersPath;

        final SparkSession.Builder builder = SparkSession.builder()
            .appName("BusinessTransactionsEmpty")
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
        final BusinessTransactionsEmpty job = new BusinessTransactionsEmpty(transactionsPath, usersPath, false);
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
        sparkContext.textFile(transactionsPath);
        sparkContext.textFile(usersPath);

        return Collections.emptyList();
    }

}

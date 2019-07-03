package com.opsian.spark_examples;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

import java.util.List;

public class BusinessTransactionsTest
{
    @Test
    public void shouldParseExampleFile()
    {
        final JavaSparkContext context = new JavaSparkContext("local", "SparkJoinsTest");

        List<Tuple2<String, String>> result = BusinessTransactions.run(
            "src/main/resources/transactions",
            "src/main/resources/users",
            context);

        Assert.assertEquals("1", result.get(0)._1);
        Assert.assertEquals("3", result.get(0)._2);
        Assert.assertEquals("2", result.get(1)._1);
        Assert.assertEquals("1", result.get(1)._2);

        context.stop();
    }
}

package com.opsian.spark_examples;

import org.junit.Test;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.concurrent.JavaConversions;
import scala.concurrent.JavaConversions$;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class BusinessTransactionsTest
{
    public static final String TRANSACTIONS = "src/main/resources/transactions";
    public static final String USERS = "src/main/resources/users";

    @Test
    public void shouldParseExampleFile()
    {
        checkResults(new BusinessTransactions(TRANSACTIONS, USERS, true)
            .run());
    }

    @Test
    public void optimizedShouldParseExampleFile()
    {
        checkResults(new BusinessTransactionsOptimized(TRANSACTIONS, USERS, true)
            .run());
    }

    @Test
    public void scalaShouldParseExampleFile()
    {
        checkResults(new BusinessTransactionsScala(TRANSACTIONS, USERS, true)
            .run());
    }

    @Test
    public void scalaOptimizedShouldParseExampleFile()
    {
        checkResults(new BusinessTransactionsScalaOptimized(TRANSACTIONS, USERS, true)
            .run());
    }

    private void checkResults(final Seq<Tuple2<String, String>> result)
    {
        checkResults(JavaConverters.seqAsJavaListConverter(result).asJava());
    }

    private void checkResults(final List<Tuple2<String, String>> result)
    {
        assertEquals("1", result.get(0)._1);
        assertEquals("3", result.get(0)._2);

        assertEquals("2", result.get(1)._1);
        assertEquals("1", result.get(1)._2);
    }
}

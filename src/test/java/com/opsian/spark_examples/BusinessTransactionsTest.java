package com.opsian.spark_examples;

import org.junit.Test;
import scala.Tuple2;

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

    private void checkResults(final List<Tuple2<String, String>> result)
    {
        assertEquals("1", result.get(0)._1);
        assertEquals("3", result.get(0)._2);

        assertEquals("2", result.get(1)._1);
        assertEquals("1", result.get(1)._2);
    }
}

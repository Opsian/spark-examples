package com.opsian.spark_examples;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class GenerateTransactions
{

    private static final int[] PRICES = {
        300,
        500,
        100,
        50,
        10,
        150,
        200,
        400,
        250,
        30
    };

    private static final String[] PRODUCTS = {
        "jumper",
        "blazer",
        "shirt",
        "trousers",
        "pants",
        "skirt",
        "blouse",
        "dress",
        "shoes",
        "scarf",
    };

    public static void main(String[] args) throws IOException
    {
        final int totalTransactions = 10_000_000;

        try (final BufferedWriter out = new BufferedWriter(new FileWriter(new File("big-transactions"))))
        {
            for (int id = 1; id <= totalTransactions; id++)
            {
                int userId = ThreadLocalRandom.current().nextInt(1, GenerateUsers.TOTAL_USERS + 1);
                final int quantity = ThreadLocalRandom.current().nextInt(1, 5);
                final int productId = ThreadLocalRandom.current().nextInt(PRODUCTS.length);

                out.write(String.format("%d\t%d\t%d\t%d\t%s%n",
                    id,
                    quantity,
                    userId,
                    PRICES[productId],
                    PRODUCTS[productId]));

                if ((id % 100) == 0)
                {
                    System.out.println(id);
                }
            }
        }
    }
}

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SimpleApp
{
    public static void main(String[] args)
    {
        String logFile = "/home/richard/Programs/spark-2.4.3-bin-hadoop2.7/README.md";
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(contains("a")).count();
        long numBs = logData.filter(contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        // spark.stop();

    }

    private static FilterFunction<String> contains(final String content)
    {
        return (FilterFunction<String>) s -> s.contains(content);
    }
}

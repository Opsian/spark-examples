package com.opsian.spark_examples

import java.io.File

import com.opsian.spark_examples.StringParser.{skipContent, skipWhitespace}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map


object BusinessTransactionsScalaOptimized {
  def main(args: Array[String]): Unit = {
    val thisDir = new File(".").getAbsolutePath + "/"
    val transactionsPath = thisDir + "big-transactions"
    val usersPath = thisDir + "big-users"
    val outputPath = thisDir + "results"
    val job = new BusinessTransactionsScalaOptimized(transactionsPath, usersPath, false)
    val output = job.run
    job.save(output, outputPath)
  }
}

class BusinessTransactionsScalaOptimized
(val transactionsPath: String, val usersPath: String, val isLocal: Boolean) {
  val builder: SparkSession.Builder = SparkSession.builder
    .appName("BusinessTransactionsScalaOptimized")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  if (isLocal) builder.master("local")
  private val spark = builder.getOrCreate
  private val sparkContext = spark.sparkContext

  def save(output: Seq[(String, String)], outputPath: String): Unit = {
    sparkContext
      .parallelize(output)
      .saveAsHadoopFile(outputPath, classOf[String], classOf[String], classOf[TextOutputFormat[_, _]])
    spark.stop()
  }

  def run: Seq[(String, String)] = {
    val transactions = parseTransactionsFile(transactionsPath, sparkContext)
    val users = parseUsersFile(usersPath, sparkContext)
    val result : Map[Int, Long] = transactions
      .leftOuterJoin(users)
      .values
      .distinct
      .map(a => (a._1, a._2.get))
      .countByKey()

    result.map(row => (row._1.toString, row._2.toString)).toSeq
  }

  protected def parseUsersFile(usersPath: String, sparkContext: SparkContext): RDD[(Int, String)] = {
    val userInputFile = sparkContext.textFile(usersPath)
    userInputFile.map(line => {
      val userSplit = line.split("\\s+")
      val userId = userSplit(0)
      val country = userSplit(3)
      (userId.toInt, country)
    })
  }

  protected def parseTransactionsFile(transactionsPath: String, sparkContext: SparkContext): RDD[(Int, Int)] = {
    val transactionInputFile = sparkContext.textFile(transactionsPath)
    transactionInputFile
      .filter(line => !line.trim.isEmpty)
      .map(line => {
        var index = 0

        // Skip column 0
        index = skipContent(line, index)

        // skip gap 0
        index = skipWhitespace(line, index)

        // get column 1
        val column1Start = index
        index = skipContent(line, index)
        val column1 = line.substring(column1Start, index)

        // skip gap 1
        index = skipWhitespace(line, index)

        // get column 2
        val column2Start = index
        index = skipContent(line, index)
        val column2 = line.substring(column2Start, index)

        val quantity = Integer.parseInt(column1)
        val userId = Integer.parseInt(column2)
        (userId, quantity)
    })
  }
}

object StringParser {
  def skipWhitespace(line: String, start: Int): Int = {
    var index = start
    while (isWhiteSpace(line, index)) {
      index += 1
    }
    index
  }

  def skipContent(line: String, start: Int): Int = {
    var index = start
    while (!isWhiteSpace(line, index)) {
      index += 1
    }
    index
  }

  def isWhiteSpace(line: String, index: Int): Boolean = {
    val ch = line.charAt(index)
    ch == ' ' || ch == '\t'
  }
}

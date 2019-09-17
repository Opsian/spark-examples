package com.opsian.spark_examples

import java.io.File

import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map

object BusinessTransactionsScala {
  def main(args: Array[String]): Unit = {
    val thisDir = new File(".").getAbsolutePath + "/"
    val transactionsPath = thisDir + "big-transactions"
    val usersPath = thisDir + "big-users"
    val outputPath = thisDir + "results"
    val job = new BusinessTransactionsScala(transactionsPath, usersPath, false)
    val output = job.run
    job.save(output, outputPath)
  }
}

class BusinessTransactionsScala(val transactionsPath: String, val usersPath: String, val isLocal: Boolean) {
  val builder: SparkSession.Builder = SparkSession.builder.appName("BusinessTransactionsScala")
  if (isLocal) builder.master("local")
  private val spark = builder.getOrCreate
  private val sparkContext = spark.sparkContext

  def save(output: Seq[(String, String)], outputPath: String): Unit = {
    sparkContext.parallelize(output)
      .saveAsTextFile(outputPath)
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
        val transactionSplit = line.split("\\s+")
        val quantity = transactionSplit(1).toInt
        val userId = transactionSplit(2).toInt
        (userId, quantity)
    })
  }
}

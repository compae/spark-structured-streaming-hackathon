package com.spark.hackathon.structured.streaming.explicacion

import java.util.concurrent.TimeUnit

import akka.event.slf4j.SLF4JLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}


object AgregacionesMain extends App with SLF4JLogging {

  /** Creating context **/

  val sparkConf = new SparkConf()
    .setAppName("structured-streaming-aggregations-tests")
    .setMaster("local[*]")
  val sparkSession = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  import sparkSession.implicits._


  /** Inicializar source socket **/

  // Reading from Socket nc -lk 9999
  val lines = sparkSession.readStream
    .format("socket")
    .option("host", "127.0.0.1")
    .option("port", 9999)
    .load()


  /** Creating aggregations to execute **/

  // Split the lines into words
  val randomGenerator = scala.util.Random
  val words = lines.as[String]
    .flatMap(lineString => lineString.split(" "))
    .withColumn("university", lit(Literal("uax")))
    .withColumn("employees", lit(Literal(randomGenerator.nextInt(10))))

  // Generate running basic counts
  val totalCounts = words
    .groupBy("university") //Necesaria una agregacion: groupBy, cube, rollup
    .agg(count('*) as "total")

  val total2Counts = words
    .groupBy("university") //Necesaria una agregacion: groupBy, cube, rollup
    .count()

  //Funciones de agregacion avg, max, min, sum, count, stddev.
  val todasAgregaciones = words.groupBy("university")
    .agg(
      "employees" -> "max",
      "employees" -> "min",
      "employees" -> "avg",
      "employees" -> "sum",
      "*" -> "count"
    )

  /** Queries a ejecutar **/

  // Second query over modified data
  val totalCountsQuery = totalCounts.writeStream
    .outputMode(OutputMode.Complete())
    .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
    .format("console")
    .queryName("totalCountsQuery")

  val total2CountsQuery = total2Counts.writeStream
    .outputMode(OutputMode.Complete())
    .format("console")
    .queryName("total2CountsQuery")

  val todasAgregacionesQuery = todasAgregaciones.writeStream
    .outputMode(OutputMode.Complete())
    //.outputMode(OutputMode.Update()) //Check with watermark
    //.outputMode(OutputMode.Append()) //Not supported in aggregations without watermark!!
    //.option("checkpointLocation", "/tmp/checkpoint")
    .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
    .format("console")
    .queryName("todasAgregacionesQuery")

  /** Arrancar queries **/

  //Only one aggregation supported over the same stream
  val totalCountsExecution = totalCountsQuery.start()
  //val total2CountsExecution = total2CountsQuery.start()
  //val todasAgregacionesExecution = todasAgregacionesQuery.start()

  /** Manejar execution **/

  totalCountsExecution.awaitTermination()
  //total2CountsExecution.awaitTermination()
  //todasAgregacionesExecution.awaitTermination()

}


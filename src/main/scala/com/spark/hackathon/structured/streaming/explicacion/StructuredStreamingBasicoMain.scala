package com.spark.hackathon.structured.streaming.explicacion

import akka.event.slf4j.SLF4JLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode


object StructuredStreamingBasicoMain extends App with SLF4JLogging {

  /** Creación de la session */

  val sparkConf = new SparkConf()
    .setAppName("structured-streaming-basic-tests")
    .setMaster("local[*]")
  val sparkSession = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  import sparkSession.implicits._


  /** Inicializar source: Socket nc -lk 9999 */

  val stringSocket = sparkSession.readStream
    .format("socket")
    .option("host", "127.0.0.1")
    .option("port", 9999)
    .load()
  stringSocket.printSchema()


  /** Creacion de queries a ejecutar (Este source solo soporta una query!!) */

  // Primera query sobre un socket
  val linesQuery = stringSocket.writeStream
    .outputMode(OutputMode.Append())
    .format("console")
    //.trigger(Trigger.Once())
    //.trigger(Trigger.ProcessingTime(6, TimeUnit.SECONDS))
    //.trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
    .queryName("lineasQuery")


  // Separar en palabras y añadir dos columnas
  val randomGenerator = scala.util.Random
  val words = stringSocket.as[String]
    .flatMap(lineString => lineString.split(" "))
    .withColumn("university", lit(Literal("uax")))
    .withColumn("employees", lit(Literal(randomGenerator.nextInt(10))))
  words.printSchema()

  // Segunda query sobre los datos modificados
  val wordsQuery = words.writeStream
    .outputMode(OutputMode.Append())
    .format("console")
    .queryName("palabrasQuery")


  /** Arrancar las queries (solo una simultanea)*/

  //val linesQueryExecution = linesQuery.start()
  val wordsQueryExecution = wordsQuery.start()


  /** Manejar la execution **/

  //linesQueryExecution.awaitTermination()
  wordsQueryExecution.awaitTermination()

}


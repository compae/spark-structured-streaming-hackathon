package com.spark.hackathon.structured.streaming.explicacion

import akka.event.slf4j.SLF4JLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object StructuredFileSourceMain extends App with SLF4JLogging {

  /** Creating context **/

  val sparkConf = new SparkConf()
    .setAppName("structured-streaming-json-file-tests")
    .setMaster("local[*]")
  //sparkConf.set("spark.sql.streaming.schemaInference", "true")
  val sparkSession = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  import sparkSession.implicits._

  /** Inicializar source de JSON **/

  case class Canciones(album: String, artist: String, genre: String, song: String)

  //Recordando formas de crear un schema
  import org.apache.spark.sql.Encoders
  val encoderSchema = Encoders.product[Canciones].schema
  val userSchema = new StructType()
    .add("album", "string")
    .add("artist", "string")
    .add("genre", "string")
    .add("song", "string")
  val otherSchemaWay = new StructType(Array(
    StructField("album", StringType),
    StructField("artist", StringType),
    StructField("genre", StringType),
    StructField("song", StringType)
  ))
  val jsonDF = sparkSession
    .readStream
    .schema(userSchema) // Specify schema of the json, opcional, pero se recomienda!!
    .json("/home/jcgarcia/hackathon/json") //path no fichero!!

  val generoPop = jsonDF.select($"album", $"artist", $"song", $"genre").where($"genre" === "Pop")
  val totalPorGenero = jsonDF.groupBy("genre").agg("*" -> "count")

  /** Queries a ejecutar **/

  val generoPopQuery = generoPop.writeStream
    .outputMode(OutputMode.Append())
    .format("console")
    .queryName("generoPopQuery")

  val totalPorGeneroQuery = totalPorGenero.writeStream
    .outputMode(OutputMode.Complete())
    .format("console")
    .queryName("totalPorGeneroQuery")

  /** Arrancar queries **/

  val generoPopExecution = generoPopQuery.start()
  val totalPorGeneroExecution = totalPorGeneroQuery.start()

  /** Manejar execution **/

  generoPopExecution.awaitTermination()
  totalPorGeneroExecution.awaitTermination()

}


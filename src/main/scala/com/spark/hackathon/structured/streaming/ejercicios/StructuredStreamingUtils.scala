package com.spark.hackathon.structured.streaming.ejercicios

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

trait StructuredStreamingUtils {

  val sparkConf: SparkConf

  val sparkSession: SparkSession

  /** Leer fichero csv de tiendas batch: */
  def leerFicheroTiendas: DataFrame

  /** Leer path donde aparecen ficheros json de tickets */
  def leerFicheroTickets: DataFrame

  /** Registrar como tablas temporales los ficheros batch leídos */
  def registrarTablas(dataFramesARegistrar: Map[String, DataFrame]): Unit

  /** Total de ventas de todos los tickets por tienda enriqueciendo con los datos de tiendas(Join tickets, stores) */
  def totalVentaPorTienda(tickets: DataFrame, tiendas: DataFrame): StreamingQuery

  /** Top 20 de registros */
  def executeStreamingQuery(streamingQuery: StreamingQuery): Unit

}

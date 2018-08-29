package com.spark.hackathon.structured.streaming.ejercicios

import akka.event.slf4j.SLF4JLogging


object SparkStructuredStreaming extends App with SLF4JLogging {


  /** Crear una implementación de SqlUtils, requisitos:
    *
    * Dar un nombre a la app de Spark
    * Asignarle un master
    * Implementar SqlUtils
    *
    */

  /*val structuredStreamingUtils = new StructuredStreamingUtilsImpl
  val tickets = structuredStreamingUtils.leerFicheroTickets
  val tiendas = structuredStreamingUtils.leerFicheroTiendas

  structuredStreamingUtils.registrarTablas(Map(
    "tiendas" -> tiendas,
    "tickets" -> tickets
  ))

  val totalVentaPorTienda = structuredStreamingUtils.totalVentaPorTienda(tickets, tiendas)

  structuredStreamingUtils.executeStreamingQuery(totalVentaPorTienda)*/

}


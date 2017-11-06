package com.utad.bigdata

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object HotelesReveneu {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("HotelesReveneu")
      .master("local[2]")
      .getOrCreate()

    runProgrammaticHotelesRevenue(spark)
    spark.stop()
  }

  private def runProgrammaticHotelesRevenue(spark: SparkSession): Unit = {

    // Crea RDDs a partir de ficheros csv
    val hotelesRDD = spark.sparkContext.textFile("/home/ros01507/Descargas/Ejercicio_Entrevista/europe.csv")
    val revenueRDD = spark.sparkContext.textFile("/home/ros01507/Descargas/Ejercicio_Entrevista/revenue.csv")

    // Aplicamos Schema
    val schemaStringH = "id;name;address;zip;city_hotel;cc1;ufi;class;currencycode;minrate;maxrate;preferred;" +
      "nr_rooms;public_ranking;hotel_url;city_unique;city_preferred;review_score;review_nr"
    val schemaStringG = "id;revenue"

    val fieldsH = schemaStringH.split(";")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schemaH = StructType(fieldsH)
    val schemaG = StructType(List(StructField("id",StringType,true), StructField("revenue",IntegerType,true)))

    // Convertimos registros del RDD en filas.Filtramos las cabeceras de los csv.
    val hotelesrowRDD = hotelesRDD
      .map(_.split(";")).filter(row => row(0) != "id")
      .map(attributes => Row(attributes(0).trim, attributes(1).trim, attributes(2).trim, attributes(3)
      .trim, attributes(4).trim,attributes(5).trim, attributes(6).trim, attributes(7).trim, attributes(8)
      .trim,attributes(9).trim, attributes(10).trim, attributes(11).trim,attributes(12).trim,attributes(13)
      .trim,attributes(14).trim,attributes(15).trim,attributes(16).trim, attributes(17).trim,attributes(18).trim))

    val revenrowRDD = revenueRDD
      .map(_.split(";"))
      .filter(row => row(0) != "id")
      .map(attributes => Row(attributes(0).trim, attributes(1).trim.toInt))

    // Creamos Dataframes
    val hotelDF = spark.createDataFrame(hotelesrowRDD, schemaH)
    val revenDF = spark.createDataFrame(revenrowRDD, schemaG)

    // Creamos tablas temporales
    hotelDF.createOrReplaceTempView("hoteles")
    revenDF.createOrReplaceTempView("revenue")

    // Guardamos en formato parquet en HDFS
    hotelDF.write.mode(SaveMode.Append).parquet("hdfs://localhost:9000/user/hoteles/europe");
    revenDF.write.mode(SaveMode.Append).parquet("hdfs://localhost:9000/user/hoteles/revenue");

    // Creamos querys en sendas tablas( filtramos hoteles de España, para posterior uso en las agregaciones
    val Hoteles = spark.sql("SELECT id,name,city_hotel,cc1 FROM hoteles").filter(row => row(3) == "es")
    val Revenue = spark.sql("SELECT * FROM revenue")


    // Obtener por pantalla los 100 hoteles que más revenue han generado.
    val queryPorRevenue = Hoteles.join(Revenue, Hoteles.col("id") === Revenue("id"), "left")
      .orderBy(desc("revenue")).show(100)

    // Obtener por pantalla las 200 ciudades con más revenue han obtenido.
    val queryPorRevenueCiudad = Hoteles.join(Revenue, Hoteles.col("id") === Revenue("id"), "left")
      .groupBy("city_hotel").agg(sum("revenue").as("TotalRevenue")).orderBy(desc("TotalRevenue")).show(200)

          }
}

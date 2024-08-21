package com.example.bigdata

import com.example.bigdata.tools.GetContext.getSparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ETLWSklepy {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: ETLWCzas <externalDataPath>")
      System.exit(1)
    }

    val externalDataPath = args(0)

    val spark = getSparkSession("ETLWSklepy")

    val sklepyDF = spark.read.format("org.apache.spark.csv")
      .option(key = "header", value = true)
      .option(key = "inferSchema", value = true)
      .csv(s"$externalDataPath/sklepy.txt").cache()

    //TODO: Dokończ implementację procesu ETL zasilającego tabelę W_SKLEPY

  }

}

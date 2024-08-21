package com.example.bigdata.tools

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object GetContext {
  def getSparkSession(appName: String): SparkSession = {
    val master = sys.props.getOrElse("spark.master", "local[*]")

    val conf: SparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)

    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark
  }
}

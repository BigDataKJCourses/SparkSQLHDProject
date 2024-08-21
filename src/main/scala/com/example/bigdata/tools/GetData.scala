package com.example.bigdata.tools

import org.apache.spark.sql.{DataFrame, SparkSession}

object GetData {

  def getSprzedaz(spark: SparkSession, mysqlURL: String, database: String,
                  mysqlUser: String, mysqlPass: String): DataFrame = {
    spark.read.format("jdbc").options(
      Map("url" -> s"$mysqlURL/$database?autoReconnect=true",
        "driver" -> "com.mysql.cj.jdbc.Driver",
        "user" -> mysqlUser,
        "password" -> mysqlPass,
        "dbtable" -> "etl_sprzedaz")).load()
  }

  def getProdukty(spark: SparkSession, mysqlURL: String, database: String,
                   mysqlUser: String, mysqlPass: String): DataFrame = {
    val produktyDF = spark.read.format("jdbc").options(
      Map("url" -> s"$mysqlURL/$database?autoReconnect=true",
        "driver" -> "com.mysql.cj.jdbc.Driver",
        "user" -> mysqlUser,
        "password" -> mysqlPass,
        "dbtable" -> "etl_produkty")).load()

    val kategorieDF = spark.read.format("jdbc").options(
      Map("url" -> s"$mysqlURL/$database?autoReconnect=true",
        "driver" -> "com.mysql.cj.jdbc.Driver",
        "user" -> mysqlUser,
        "password" -> mysqlPass,
        "dbtable" -> "etl_kategorie")).load()

    val departamentyDF = spark.read.format("jdbc").options(
      Map("url" -> s"$mysqlURL/$database?autoReconnect=true",
        "driver" -> "com.mysql.cj.jdbc.Driver",
        "user" -> mysqlUser,
        "password" -> mysqlPass,
        "dbtable" -> "etl_departamenty")).load()

    val tempProduktyDF = produktyDF.
      join(kategorieDF, produktyDF("p_k_id").equalTo(kategorieDF("k_id")), "leftouter").
      join(departamentyDF, produktyDF("p_d_id").equalTo(departamentyDF("d_id")), "leftouter")

    tempProduktyDF
  }



}

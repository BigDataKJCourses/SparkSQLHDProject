package com.example.bigdata

import com.databricks.spark.xml.XmlDataFrameReader
import com.example.bigdata.tools.GetData.getSprzedaz
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object ETLWCzas {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("Usage: ETLWCzas <externalDataPath> <mysqlURL> <mysqlUser> <mysqlPass>")
      System.exit(1)
    }

    val externalDataPath = args(0)
    val mysqlURL = args(1)
    val mysqlUser = args(2)
    val mysqlPass = args(3)

    val master = sys.props.getOrElse("spark.master", "local[*]")

    val conf: SparkConf = new SparkConf()
      .setAppName("SparkWordCount")
      .setMaster(master)

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val dniWolneDF = spark.read.
      format("com.databricks.spark.xml").
      option("rootTag", "DNI_WOLNE").
      option("rowTag", "DATA").
      xml(s"$externalDataPath/dni_wolne.xml")

    val eastDatyDF = getSprzedaz(spark, mysqlURL, "east", mysqlUser, mysqlPass)
      .select("s_data")
    val westDatyDF = getSprzedaz(spark, mysqlURL, "west", mysqlUser, mysqlPass)
      .select("s_data")
    val centralDatyDF = getSprzedaz(spark, mysqlURL, "central", mysqlUser, mysqlPass)
      .select("s_data")

    val allDatyDF = centralDatyDF.union(eastDatyDF).union(westDatyDF).distinct()

    val tempDatyDF = allDatyDF.join(dniWolneDF,
      allDatyDF("s_data").equalTo(dniWolneDF("DZIEN")),
      "leftouter")

    val getYear: String => Integer = _.substring(0,4).toInt
    val getMonth: String => Integer = _.substring(5,7).toInt
    val isFree: String => Boolean = _ != null

    val getYearUDF = udf(getYear)
    val getMonthUDF = udf(getMonth)
    val isFreeUDF = udf(isFree)

    val datyDF = tempDatyDF.
      withColumn("rok", getYearUDF($"s_data")).
      withColumn("miesiac", getMonthUDF($"s_data")).
      withColumn("data", $"s_data").
      withColumn("czy_wolny", isFreeUDF($"DZIEN")).
      drop("DZIEN","s_data")

    datyDF.write.mode("overwrite").insertInto("w_czas")
  }
}

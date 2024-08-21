package com.example.bigdata

import com.example.bigdata.tools.GetContext.getSparkSession
import com.example.bigdata.tools.GetData.getProdukty
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ETLWProdukty {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: ETLWProdukty <mysqlURL> <mysqlUser> <mysqlPass>")
      System.exit(1)
    }

    val mysqlURL = args(0)
    val mysqlUser = args(1)
    val mysqlPass = args(2)

    val spark = getSparkSession("ETLWProdukty")

    val eastProduktyDF = getProdukty(spark, mysqlURL, "east", mysqlUser, mysqlPass)
    val westProduktyDF = getProdukty(spark, mysqlURL, "west", mysqlUser, mysqlPass)
    val centralProduktyDF = getProdukty(spark, mysqlURL, "central", mysqlUser, mysqlPass)

    eastProduktyDF.createTempView("eastProduktyDF")
    westProduktyDF.createTempView("westProduktyDF")
    centralProduktyDF.createTempView("centralProduktyDF")

    spark.sql("""insert overwrite table W_PRODUKTY
    select distinct p_nazwa as nazwa_produktu,
        k_nazwa as nazwa_kategorii,
        d_nazwa as nazwa_departamentu,
        case p_t_id WHEN 1 THEN 'Filmy'
            WHEN 2 THEN 'Gry'
            ELSE 'Żywność'
        END as typ,
        p_id as id_produktu
    from (
        select * from eastProduktyDF
        union
        select * from westProduktyDF
        union
        select * from centralProduktyDF)""")

  }

}

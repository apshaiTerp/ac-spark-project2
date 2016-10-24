package com.ac.umkc.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * @author AC010168
 *
 */
public class SparkDriver {

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      SparkSession spark = SparkSession.builder()
          .master("local")
          .appName("Java SparkDriver")
          .config("spark.some.config.option", "some-value")
          .getOrCreate();
      
      Dataset<Row> df = spark.read().json(args[0]);
      
      df.show();

      df.printSchema();

    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

}

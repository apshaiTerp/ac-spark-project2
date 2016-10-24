package com.ac.umkc.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
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
      //df.show();
      df.printSchema();
      
      //New Approach for RDD
      JavaRDD<TwitterUser> userRDD = spark.read().textFile(args[0]).javaRDD().map(
          new Function<String, TwitterUser>() {
            /** It wants it, so I gave it one */
            private static final long serialVersionUID = 5654145143753968626L;

            public TwitterUser call(String line) throws Exception {
              TwitterUser user = new TwitterUser();
              user.parseFromJSON(line);
              return user;
            }
            
          });
      Dataset<Row> userDF = spark.createDataFrame(userRDD, TwitterUser.class);
      userDF.createOrReplaceTempView("users");
      
      Dataset<Row> resultsDF = spark.sql("SELECT userType, count(*) FROM users GROUP BY userType");
      resultsDF.show();

    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

}

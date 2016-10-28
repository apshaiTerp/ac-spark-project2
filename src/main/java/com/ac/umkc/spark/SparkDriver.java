package com.ac.umkc.spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Serializable;
import scala.Tuple2;

import com.ac.umkc.spark.util.LocationSorter;


/**
 * @author AC010168
 *
 */
public class SparkDriver implements Serializable {
  
  /** Given because it wants it */
  private static final long serialVersionUID = 2332538165677195031L;
  
  /** The users file location */
  private String userPath;
  /** The tweets file location */
  private String tweetPath;

  /** Reference to the valid SparkSession, which needs to be created in main() */
  private SparkSession sparkSession;
  
  /**
   * This is the basic constructor
   * @param userPath the path to the user content in HDFS
   * @param tweetPath the path to the tweet content in HDFS
   * @param sparkSession Reference to the current spark session
   */
  public SparkDriver(String userPath, String tweetPath, SparkSession sparkSession) {
    this.userPath     = userPath;
    this.tweetPath    = tweetPath;
    this.sparkSession = sparkSession;
  }
  
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
      
      SparkDriver driver = new SparkDriver(args[0], args[1], spark);
      driver.execute();
      
      
      
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }
  
  public void execute() {
    
    executeQuery1();
    executeQuery2();
    executeQuery3();
  }
  
  /**
   * This method should help us generate (and print) the top 10 most popular locations
   * for gamers.  This should only require the user data.  The gist of this query is
   * 'What are the top ten most popular locations where gamers are located'
   */
  private void executeQuery1() {
    System.out.println ("*************************************************************************");
    System.out.println ("***************************  Execute Query 1  ***************************");
    System.out.println ("*************************************************************************");
    
    //New Approach for RDD
    JavaRDD<TwitterUser> userRDD = sparkSession.read().textFile(userPath).javaRDD().map(
        new Function<String, TwitterUser>() {
          /** It wants it, so I gave it one */
          private static final long serialVersionUID = 5654145143753968626L;

          public TwitterUser call(String line) throws Exception {
            TwitterUser user = new TwitterUser();
            user.parseFromJSON(line);
            return user;
          }
        }).filter(new Function<TwitterUser, Boolean>() {
          /** It wants it, so I gave it one */
          private static final long serialVersionUID = -2462072955148041130L;

          public Boolean call(TwitterUser user) {
            return user.getLocation().length() > 0;
          }
        });
    
    JavaPairRDD<String, Integer> locations = userRDD.mapToPair(new PairFunction<TwitterUser, String, Integer>() {
      /** Gave it cause it wants one. */
      private static final long serialVersionUID = 7711668945522265992L;

          public Tuple2<String, Integer> call(TwitterUser user) {
            return new Tuple2<String, Integer>(user.getLocation(), 1);
          }
      });
    
    JavaPairRDD<String, Integer> sortLocations = locations.reduceByKey(new Function2<Integer, Integer, Integer>() {
      /** Gave it cause it wants one. */
      private static final long serialVersionUID = 1758905397312207150L;

          public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
      }).sortByKey();
    
    List<Tuple2<String, Integer>> results = sortLocations.takeOrdered(10, new LocationSorter());
    for (Tuple2<String, Integer> tuple : results)
      System.out.println ("(" + tuple._1() + "," + tuple._2() + ")");

    System.out.println ("-------------------------------------------------------------------------");
    System.out.println ("-----------------------------  End Query 1  -----------------------------");
    System.out.println ("-------------------------------------------------------------------------");
  }
  
  /**
   * This method should help us generate (and print) the top X most popular users.  
   * This should only require the twitter data.  The gist of this query is
   * 'Most popular users (based on likes and retweets per tweet as an average)'
   */
  private void executeQuery2() {
    System.out.println ("*************************************************************************");
    System.out.println ("***************************  Execute Query 2  ***************************");
    System.out.println ("*************************************************************************");
    
    //Can do this in RDD or DataFrames
    JavaRDD<TwitterStatus> tweetRDD = sparkSession.read().textFile(tweetPath).javaRDD().map(
        new Function<String, TwitterStatus>() {
          
          /** It wants it, so I gave it one */
          private static final long serialVersionUID = 1503107307123339206L;

          public TwitterStatus call(String line) throws Exception {
            TwitterStatus status = new TwitterStatus();
            status.parseFromJSON(line);
            return status;
          }
        });
    
    Dataset<Row> tweetDF = sparkSession.createDataFrame(tweetRDD, TwitterStatus.class);
    tweetDF.createOrReplaceTempView("tweets");
    
    Dataset<Row> resultsDF = sparkSession.sql(
        "SELECT userName, AVG(favoriteCount), AVG(retweetCount), SUM(favoriteCount), SUM(retweetCount), COUNT(statusID) from tweets " + 
        "GROUP BY userName ORDER BY AVG(retweetCount) DESC");
    
    resultsDF.show();

    System.out.println ("-------------------------------------------------------------------------");
    System.out.println ("-----------------------------  End Query 2  -----------------------------");
    System.out.println ("-------------------------------------------------------------------------");
  }
  
  /**
   * This method should help us generate (and print) the most commonly used hashtags
   * per user group.  The gist of this query is 'Most common hashtags used per user 
   * group (requires a join between user and tweet data sets?)'
   */
  private void executeQuery3() {
    System.out.println ("*************************************************************************");
    System.out.println ("***************************  Execute Query 3  ***************************");
    System.out.println ("*************************************************************************");
    
    //Convert our tweet file into objects, then filter down to only tweets with Hash Tags
    JavaRDD<TwitterStatus> tweetRDD = sparkSession.read().textFile(tweetPath).javaRDD().map(
        new Function<String, TwitterStatus>() {
          
          /** It wants it, so I gave it one */
          private static final long serialVersionUID = 1503107307123339206L;

          public TwitterStatus call(String line) throws Exception {
            TwitterStatus status = new TwitterStatus();
            status.parseFromJSON(line);
            return status;
          }
        }).filter(        
        new Function<TwitterStatus, Boolean>() {
      /** It wants it, so I gave it one */
      private static final long serialVersionUID = 113462456123339206L;

      public Boolean call(TwitterStatus status) throws Exception {
        return ((status.getHashTags() != null) && (status.getHashTags().size() > 0));
      }
    });

    //Flat map our individual hashTags to Tuples of (hashTag, count), then
    //run the aggregating reduce operation, and sort in descending order
    JavaPairRDD<String, Integer> hashTags = tweetRDD.flatMapToPair(
        new PairFlatMapFunction<TwitterStatus, String, Integer>() {
          /** It wants it, so I gave it one */
          private static final long serialVersionUID = 6310698767617690806L;

          public Iterator<Tuple2<String, Integer>> call(TwitterStatus status) {
            List<Tuple2<String, Integer>> results = new ArrayList<Tuple2<String, Integer>>(status.getHashTags().size());
            for (String hashTag : status.getHashTags())
              results.add(new Tuple2<String, Integer>(hashTag, 1));
            return results.iterator();
          }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
          /** It wants it, so I gave it one */
          private static final long serialVersionUID = -4583081102611123090L;

          public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
        }).sortByKey(false);
    
    List<Tuple2<String, Integer>> results = hashTags.take(10);
    System.out.println ("The Top 10 HashTags in use are:");
    int count = 0;
    
    for (Tuple2<String, Integer> tuple : results) {
      count++;
      System.out.println (count + ")  " + tuple._1() + "  (" + tuple._2() + ")");
    }
    
    System.out.println ("-------------------------------------------------------------------------");
    System.out.println ("-----------------------------  End Query 3  -----------------------------");
    System.out.println ("-------------------------------------------------------------------------");
  }

  /**
   * This method should be used to help us generate (and print) the tweet frequency
   * grouped by day and user group for a given hashtag.  The gist of this query is 
   * 'Tweet frequency (per day)for a single hashtag (#GenCon) - Partition along date?'
   */
  private void executeQuery4() {
    System.out.println ("*************************************************************************");
    System.out.println ("***************************  Execute Query 4  ***************************");
    System.out.println ("*************************************************************************");
    
    
    
    System.out.println ("-------------------------------------------------------------------------");
    System.out.println ("-----------------------------  End Query 4  -----------------------------");
    System.out.println ("-------------------------------------------------------------------------");
  }
  
  /**
   * This method should be used to help us generate (and print) the last X tweets that
   * mention a given topic as found in the twitter text.  The gist of this query is 
   * 'Last X Tweets referencing a given game (i.e. Terraforming Mars)'
   */
  private void executeQuery5() {
    System.out.println ("*************************************************************************");
    System.out.println ("***************************  Execute Query 5  ***************************");
    System.out.println ("*************************************************************************");
    
    
    
    System.out.println ("-------------------------------------------------------------------------");
    System.out.println ("-----------------------------  End Query 5  -----------------------------");
    System.out.println ("-------------------------------------------------------------------------");
  }
  
  @SuppressWarnings("unused")
  private void dumpMethod() {
    Dataset<Row> df = sparkSession.read().json(userPath);
    
    //df.show();
    df.printSchema();
    
    //New Approach for RDD
    JavaRDD<TwitterUser> userRDD = sparkSession.read().textFile(userPath).javaRDD().map(
        new Function<String, TwitterUser>() {
          /** It wants it, so I gave it one */
          private static final long serialVersionUID = 5654145143753968626L;

          public TwitterUser call(String line) throws Exception {
            TwitterUser user = new TwitterUser();
            user.parseFromJSON(line);
            return user;
          }
          
        });
    Dataset<Row> userDF = sparkSession.createDataFrame(userRDD, TwitterUser.class);
    userDF.createOrReplaceTempView("users");
    
    Dataset<Row> resultsDF = sparkSession.sql("SELECT userType, count(*) FROM users GROUP BY userType ORDER BY count(*) desc");
    resultsDF.show();
    
    
    JavaRDD<TwitterStatus> tweetRDD = sparkSession.read().textFile(tweetPath).javaRDD().map(
        new Function<String, TwitterStatus>() {
          
          /** It wants it, so I gave it one */
          private static final long serialVersionUID = 1503107307123339206L;

          public TwitterStatus call(String line) throws Exception {
            TwitterStatus status = new TwitterStatus();
            status.parseFromJSON(line);
            return status;
          }
        });
    
    Dataset<Row> tweetDF = sparkSession.createDataFrame(tweetRDD, TwitterStatus.class);
    tweetDF.createOrReplaceTempView("tweets");
    
    tweetDF.show();
  }

}

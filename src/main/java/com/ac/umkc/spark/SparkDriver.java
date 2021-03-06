package com.ac.umkc.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Serializable;
import scala.Tuple2;

import com.ac.umkc.spark.data.GoogleData;
import com.ac.umkc.spark.data.HashTagData;
import com.ac.umkc.spark.data.TweetLRData;
import com.ac.umkc.spark.data.TweetsDayData;
import com.ac.umkc.spark.data.TwitterStatus;
import com.ac.umkc.spark.data.TwitterStatusTopX;
import com.ac.umkc.spark.data.TwitterUser;
import com.ac.umkc.spark.util.GoogleCall;
import com.ac.umkc.spark.util.TupleSorter;

/**
 * The main class for this project.  We are leveraging Spark to run 5 queries.
 * This class is designed to be run either interactively, or non-interactively
 * using a pre-defined set of queries.
 * 
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
   * System main method.  We accept 2 or 3 parameters:
   * <ul><li>args[0] = HDFS file path of our user data file</li>
   * <li>args[1] = HDFS file path for our tweet content</li>
   * <li>args[2] (Optional) = Flag to indicate we should use the non-interactive execution path</li>
   * </ul>
   * 
   * @param args The command line paramters.  We are expecting to receive 2 or 3 entries.
   */
  public static void main(String[] args) {
    if (args.length < 2)
      throw new RuntimeException("Insufficient Command Line Arguments.  Two files paths are required.");
      
    try {
      SparkSession spark = SparkSession.builder()
          .master("local")
          .appName("Java SparkDriver")
          .config("spark.some.config.option", "some-value")
          .getOrCreate();
      
      SparkDriver driver = new SparkDriver(args[0], args[1], spark);
      
      if (args.length == 2)
        driver.execute();
      if (args.length == 3)
        driver.executeNonInteractive();
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }
  
  /**
   * Main Method for executing our game logic.
   */
  public void execute() {
    Scanner reader = new Scanner(System.in);
    reader.useDelimiter("\\n");
    
    while (true) {
      System.out.println ("Welcome to the Board Game Twitter Query Utility.  Please choose from the Options Below:");
      System.out.println ("---------------------------------------------------------------------------------------");
      System.out.println ("(1) Top 10 Locations where Board Gamers live.");
      System.out.println ("(2) Most Popular Users (based on likes and retweets) for a provided time range.");
      System.out.println ("(3) Most Commonly Used HashTags by Board Gamers for a provided time range.");
      System.out.println ("(4) Times per Day Board Game Users Groups use a provided HashTag.");
      System.out.println ("(5) Last X Tweets among Board Gamers that reference a provided Game or Term");
      System.out.println ("(X) Exit Program");
      
      System.out.print ("Your Choice: ");
      String choice = reader.next();
      
      if (choice.equalsIgnoreCase("X"))
        break;
      else if (choice.equalsIgnoreCase("1"))
        executeQuery1();
      else if (choice.equalsIgnoreCase("2")) {
        System.out.print ("Please enter a Start Date (as YYYY.MM.DD): ");
        final String startDate = reader.next();
        System.out.print ("Please enter an End Date  (as YYYY.MM.DD): ");
        final String endDate   = reader.next();
        executeQuery2(startDate, endDate);
      } else if (choice.equalsIgnoreCase("3")) {
        System.out.print ("Please enter a Start Date (as YYYY.MM.DD): ");
        final String startDate = reader.next();
        System.out.print ("Please enter an End Date  (as YYYY.MM.DD): ");
        final String endDate   = reader.next();
        executeQuery3(startDate, endDate);
      } else if (choice.equalsIgnoreCase("4")) {
        System.out.print ("Please enter a HashTag (Do not include the #): ");
        final String hashTag = reader.next();
        System.out.print ("Please enter a Start Date (as YYYY.MM.DD): ");
        final String startDate = reader.next();
        System.out.print ("Please enter an End Date  (as YYYY.MM.DD): ");
        final String endDate   = reader.next();
        executeQuery4(hashTag, startDate, endDate);
      } else if (choice.equalsIgnoreCase("5")) {
        System.out.print ("Please enter a Search Term (You may include HashTags): ");
        String searchTerm = reader.next();
        System.out.print ("How many results do you want returned: ");
        final int rowLimit = reader.nextInt();
        executeQuery5(searchTerm, rowLimit);
      } else {
        System.out.println ("The provided choice was not valid.");
      }
    }
    reader.close();
  }
  
  /**
   * Helper method to allow me to run the program in a non-interactive fashion to gather logging data
   */
  public void executeNonInteractive() {
    String results = null;
    results = executeQuery1();
    System.out.println ("\nJSON Results:\n" + results);
    
    results = executeQuery2("2016.01.01", "2016.10.31");
    System.out.println ("\nJSON Results:\n" + results);
    
    results = executeQuery3("2016.01.01", "2016.10.31");
    System.out.println ("\nJSON Results:\n" + results);
    
    results = executeQuery4("GenCon2016", "2016.01.01", "2016.10.31");
    System.out.println ("\nJSON Results:\n" + results);
    
    results = executeQuery5("Terraforming Mars", 20);
    System.out.println ("\nJSON Results:\n" + results);
  }

  /**
   * This method should help us generate (and print) the top 10 most popular locations
   * for gamers.  This should only require the user data.  The gist of this query is
   * 'What are the top ten most popular locations where gamers are located'
   * 
   * @return a JSON-formatted object containing the results
   */
  @SuppressWarnings("resource")
  private String executeQuery1() {
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
    
    JavaPairRDD<String, Integer> reduceLocations = locations.reduceByKey(new Function2<Integer, Integer, Integer>() {
      /** Gave it cause it wants one. */
      private static final long serialVersionUID = 1758905397312207150L;

          public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
      });
    
    List<Tuple2<String, Integer>> candidateList = reduceLocations.takeOrdered(2000, new TupleSorter());
    
    JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());
    JavaPairRDD<String, Integer> filteredLocations = context.parallelizePairs(candidateList);
    
    //Because the Google API has limits, and there may be duplicates near the top because of
    //the fact that location is user entered.
    JavaPairRDD<String, Integer> googleMap = filteredLocations.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
      /** Gave it cause it wants one. */
      private static final long serialVersionUID = 1L;

      public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) {
        
        GoogleData data = GoogleCall.getGoogleLocation(tuple._1());
        if (data == null) return new Tuple2<String, Integer>(tuple._1(), tuple._2());
        else return new Tuple2<String, Integer>(data.getLocation(), tuple._2());
      }
    });
    
    JavaPairRDD<String, Integer> reduceGoogle = googleMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
      /** Gave it cause it wants one. */
      private static final long serialVersionUID = 15891246245607150L;

          public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
      });
    
    //List<Tuple2<String, Integer>> results = reduceGoogle.takeOrdered(10, new TupleSorter());
    List<Tuple2<String, Integer>> results = reduceGoogle.takeOrdered(25, new TupleSorter());
    
    List<GoogleData> googleData = new ArrayList<GoogleData>(results.size());
    for (Tuple2<String, Integer> tuple : results) {
      GoogleData data = GoogleCall.getGoogleLocation(tuple._1());
      if (data == null) {
        data = new GoogleData();
        data.setLocation(tuple._1());
      }
      data.setCount(tuple._2());

      googleData.add(data);
    }
    
    String dynamicPath = "hdfs://localhost:9000/proj3/query1";
    try {
      Configuration hdfsConfiguration = new Configuration();
      hdfsConfiguration.set("fs.defaultFS", "hdfs://localhost:9000");
      FileSystem hdfs                 = FileSystem.get(hdfsConfiguration);
      
      Path checkFile = new Path(dynamicPath);
      if (hdfs.exists(checkFile)) {
        System.out.println ("I need to purge before writing!");
        hdfs.delete(checkFile, true);
      } 
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    JavaRDD<GoogleData> googleRDD = context.parallelize(googleData);
    googleRDD.saveAsTextFile(dynamicPath);
    
    String resultJSON = "{\"results\":[";
    int resultCount = 0;
    for (Tuple2<String, Integer> tuple : results) {
      resultCount++;
      String line = "{\"location\":\"" + tuple._1() + "\", \"count\":" + tuple._2() + "}";
      System.out.println (line);
      resultJSON += line;
      if (resultCount < results.size()) resultJSON += ",";
    }
    resultJSON += "]}";
    
    System.out.println ("-------------------------------------------------------------------------");
    System.out.println ("-----------------------------  End Query 1  -----------------------------");
    System.out.println ("-------------------------------------------------------------------------");
    
    return resultJSON;
  }
  
  /**
   * This method should help us generate (and print) the top 10 most popular users by userType.  
   * This requires both tweet and user data.  The gist of this query is
   * 'Most top 10 popular users (based on likes and retweets per tweet as an average) by category.
   * 
   * @param startDate The beginning date for our date range (inclusive)
   * @param endDate The ending date for our date range (inclusive)
   * 
   * @return a JSON-formatted object containing the results
   */
  @SuppressWarnings("resource")
  private String executeQuery2(String startDate, String endDate) {
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

    Dataset<Row> resultsDF = sparkSession.sql(
        "SELECT u.userType, u.screenName, u.userName, AVG(t.favoriteCount + t.retweetCount), COUNT(t.statusID) " + 
        "FROM tweets t " +
        "JOIN users u " + 
        "ON t.userName = u.screenName " + 
        "WHERE t.shortDate >= '" + startDate + "' and t.shortDate <= '" + endDate + "' " +
        "GROUP BY u.userType, u.screenName, u.userName " + 
        "ORDER BY AVG(t.favoriteCount + t.retweetCount) DESC");

    //Let's sample the top 10 most popular from each group
    List<Row> designerList   = resultsDF.filter(new Column("userType").equalTo("DESIGNER")).takeAsList(10);
    List<Row> publisherList  = resultsDF.filter(new Column("userType").equalTo("PUBLISHER")).takeAsList(10);
    List<Row> reviewerList   = resultsDF.filter(new Column("userType").equalTo("REVIEWER")).takeAsList(10);
    List<Row> conventionList = resultsDF.filter(new Column("userType").equalTo("CONVENTION")).takeAsList(10);
    List<Row> communityList  = resultsDF.filter(new Column("userType").equalTo("COMMUNITY")).takeAsList(10);
    
    List<TweetLRData> fullResults = new LinkedList<TweetLRData>();
    
    //Process all designers
    for (Row row : designerList) {
      TweetLRData rowData = new TweetLRData();
      rowData.setUserType(row.getString(0));
      rowData.setScreenName(row.getString(1));
      rowData.setUserName(row.getString(2));
      rowData.setAverageLR(row.getDouble(3));
      rowData.setTweetCount((int)row.getLong(4));
      fullResults.add(rowData);
    }

    //Process all publishers
    for (Row row : publisherList) {
      TweetLRData rowData = new TweetLRData();
      rowData.setUserType(row.getString(0));
      rowData.setScreenName(row.getString(1));
      rowData.setUserName(row.getString(2));
      rowData.setAverageLR(row.getDouble(3));
      rowData.setTweetCount((int)row.getLong(4));
      fullResults.add(rowData);
    }

    //Process all reviewers
    for (Row row : reviewerList) {
      TweetLRData rowData = new TweetLRData();
      rowData.setUserType(row.getString(0));
      rowData.setScreenName(row.getString(1));
      rowData.setUserName(row.getString(2));
      rowData.setAverageLR(row.getDouble(3));
      rowData.setTweetCount((int)row.getLong(4));
      fullResults.add(rowData);
    }

    //Process all conventions
    for (Row row : conventionList) {
      TweetLRData rowData = new TweetLRData();
      rowData.setUserType(row.getString(0));
      rowData.setScreenName(row.getString(1));
      rowData.setUserName(row.getString(2));
      rowData.setAverageLR(row.getDouble(3));
      rowData.setTweetCount((int)row.getLong(4));
      fullResults.add(rowData);
    }

    //Process all community users
    for (Row row : communityList) {
      TweetLRData rowData = new TweetLRData();
      rowData.setUserType(row.getString(0));
      rowData.setScreenName(row.getString(1));
      rowData.setUserName(row.getString(2));
      rowData.setAverageLR(row.getDouble(3));
      rowData.setTweetCount((int)row.getLong(4));
      fullResults.add(rowData);
    }
    
    String dynamicPath = "/proj3/query2/" + startDate + "/" + endDate;
    try {
      Configuration hdfsConfiguration = new Configuration();
      hdfsConfiguration.set("fs.defaultFS", "hdfs://localhost:9000");
      FileSystem hdfs                 = FileSystem.get(hdfsConfiguration);
      
      Path checkFile = new Path(dynamicPath);
      if (hdfs.exists(checkFile)) {
        System.out.println ("I need to purge before writing!");
        hdfs.delete(checkFile, true);
      } 
    } catch (IOException e) {
      e.printStackTrace();
    }

    String outputPath = "hdfs://localhost:9000" + dynamicPath;
    JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());
    JavaRDD<TweetLRData> resultRDD = context.parallelize(fullResults);
    resultRDD.saveAsTextFile(outputPath);
    
    System.out.println ("Query Results Written to: " + outputPath);
    
    System.out.println ("-------------------------------------------------------------------------");
    System.out.println ("-----------------------------  End Query 2  -----------------------------");
    System.out.println ("-------------------------------------------------------------------------");
    
    return "{}";
  }
  
  /**
   * This method should help us generate (and print) the most commonly used hashtags.  The gist of this 
   * query is 'Most common hashtags used during the provided date range.'
   * 
   * @param startDate The beginning date for our date range (inclusive)
   * @param endDate The ending date for our date range (inclusive)
   * 
   * @return a JSON-formatted object containing the results
   */
  @SuppressWarnings("resource")
  private String executeQuery3(final String startDate, final String endDate) {
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
            if ((status.getShortDate().compareTo(startDate) >= 0) && (status.getShortDate().compareTo(endDate) <= 0))
              return ((status.getHashTags() != null) && (status.getHashTags().size() > 0));
            return false;
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
        });
    
    int hashTagCount = (int)hashTags.count();
    System.out.println ("How many HashTags are in the set: " + hashTagCount);
    HashTagData hashTagCountData = new HashTagData("All HashTags", hashTagCount);
    
    JavaPairRDD<String, Integer> hashTagSum = hashTags.mapToPair(
        new PairFunction<Tuple2<String, Integer>, String, Integer>() {
          /** It wants it, so I gave it one */
          private static final long serialVersionUID = 6310698767617690806L;

          public Tuple2<String, Integer> call(Tuple2<String, Integer> status) {
            return new Tuple2<String, Integer>("All HashTags Used", status._2());
          }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
          /** It wants it, so I gave it one */
          private static final long serialVersionUID = -4583081102611123090L;

          public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
        });

    List<Tuple2<String, Integer>> otherResults = hashTagSum.collect();
    int totalHashTagsUsed = otherResults.get(0)._2();
    
    List<HashTagData> allHashTagResults = new ArrayList<HashTagData>();
    allHashTagResults.add(hashTagCountData);
    
    //Take the top 10 ordered results
    List<Tuple2<String, Integer>> results = hashTags.takeOrdered(10, new TupleSorter());
    System.out.println ("The Top 10 HashTags in use are:");
    
    for (Tuple2<String, Integer> tuple : results) {
      HashTagData curData = new HashTagData(tuple._1(), tuple._2());
      totalHashTagsUsed -= tuple._2();
      allHashTagResults.add(curData);
    }
    allHashTagResults.add(new HashTagData("Other HashTags", totalHashTagsUsed));
    
    String dynamicPath = "/proj3/query3/" + startDate + "/" + endDate;
    try {
      Configuration hdfsConfiguration = new Configuration();
      hdfsConfiguration.set("fs.defaultFS", "hdfs://localhost:9000");
      FileSystem hdfs                 = FileSystem.get(hdfsConfiguration);
      
      Path checkFile = new Path(dynamicPath);
      if (hdfs.exists(checkFile)) {
        System.out.println ("I need to purge before writing!");
        hdfs.delete(checkFile, true);
      } 
    } catch (IOException e) {
      e.printStackTrace();
    }

    String outputPath = "hdfs://localhost:9000" + dynamicPath;
    JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());
    JavaRDD<HashTagData> resultRDD = context.parallelize(allHashTagResults);
    resultRDD.saveAsTextFile(outputPath);
    
    System.out.println ("Query Results Written to: " + outputPath);
    
    System.out.println ("-------------------------------------------------------------------------");
    System.out.println ("-----------------------------  End Query 3  -----------------------------");
    System.out.println ("-------------------------------------------------------------------------");
    
    return "{}";
  }

  /**
   * This method should be used to help us generate (and print) the tweet frequency
   * grouped by day and user group for a given hashtag.  The gist of this query is 
   * 'Tweet frequency (per day)for a single hashtag (#GenCon) - Partition along date?'
   * 
   * @param searchTerm the hashTag we want to find
   * @param startDate The beginning date for our date range (inclusive)
   * @param endDate The ending date for our date range (inclusive)
   * 
   * @return a JSON-formatted object containing the results
   */
  @SuppressWarnings("resource")
  private String executeQuery4(final String searchTerm, final String startDate, final String endDate) {
    System.out.println ("*************************************************************************");
    System.out.println ("***************************  Execute Query 4  ***************************");
    System.out.println ("*************************************************************************");
    
    //Open our dataset, then filter out to matching hash tags
    JavaRDD<TwitterStatus> tweetRDD = sparkSession.read().textFile(tweetPath).javaRDD().map(
        new Function<String, TwitterStatus>() {
          
          /** It wants it, so I gave it one */
          private static final long serialVersionUID = 1503107307123339206L;

          public TwitterStatus call(String line) throws Exception {
            TwitterStatus status = new TwitterStatus();
            status.parseFromJSON(line);
            return status;
          }
        }).filter(new Function<TwitterStatus, Boolean>() {
              /** It wants it, so I gave it one */
              private static final long serialVersionUID = 113462456123339206L;

              public Boolean call(TwitterStatus status) throws Exception {
                if ((status.getShortDate().compareTo(startDate) >= 0) && (status.getShortDate().compareTo(endDate) <= 0)) {
                  boolean found = false;
                  for (String compareTerm : status.getHashTags()) {
                    if (searchTerm.equalsIgnoreCase(compareTerm))
                      found = true;
                  }
                  return found;
                }
                return false;
              }
         });

    Dataset<Row> tweetDF = sparkSession.createDataFrame(tweetRDD, TwitterStatus.class);
    tweetDF.createOrReplaceTempView("tweets");
    
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
    
    Dataset<Row> resultsDF = sparkSession.sql("Select u.userType, t.shortDate, t.year, t.month, t.day, count(t.statusID) " +
        "FROM tweets t " + 
        "JOIN users u " +
        "ON t.userID = u.twitterID " + 
        "GROUP BY u.userType, t.shortDate, t.year, t.month, t.day " + 
        "ORDER BY u.userType, t.shortDate");
    
    List<Row> results = resultsDF.collectAsList();
    List<TweetsDayData> tweetResults = new ArrayList<TweetsDayData>(results.size());
    
    for (Row row : results) {
      TweetsDayData data = new TweetsDayData();
      data.setUserType(row.getString(0));
      data.setShortDate(row.getString(1));
      data.setYear(row.getInt(2));
      data.setMonth(row.getInt(3));
      data.setDay(row.getInt(4));
      data.setCount((int)row.getLong(5));
      tweetResults.add(data);
    }
    
    String dynamicPath = "/proj3/query4/" + searchTerm + "/" + startDate + "/" + endDate;
    try {
      Configuration hdfsConfiguration = new Configuration();
      hdfsConfiguration.set("fs.defaultFS", "hdfs://localhost:9000");
      FileSystem hdfs                 = FileSystem.get(hdfsConfiguration);
      
      Path checkFile = new Path(dynamicPath);
      if (hdfs.exists(checkFile)) {
        System.out.println ("I need to purge before writing!");
        hdfs.delete(checkFile, true);
      } 
    } catch (IOException e) {
      e.printStackTrace();
    }

    String outputPath = "hdfs://localhost:9000" + dynamicPath;
    JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());
    JavaRDD<TweetsDayData> resultRDD = context.parallelize(tweetResults);
    resultRDD.saveAsTextFile(outputPath);
    
    System.out.println ("Query Results Written to: " + outputPath);
    
    System.out.println ("-------------------------------------------------------------------------");
    System.out.println ("-----------------------------  End Query 4  -----------------------------");
    System.out.println ("-------------------------------------------------------------------------");

    return "{}";
  }
  
  /**
   * This method should be used to help us generate (and print) the last X tweets that
   * mention a given topic as found in the twitter text.  The gist of this query is 
   * 'Last X Tweets referencing a given game (i.e. Terraforming Mars)'
   * 
   * @param searchTerm the term we want to search for
   * @param termLimit the number of terms we want to find.
   * 
   * @return a JSON-formatted object containing the results
   */
  @SuppressWarnings("resource")
  private String executeQuery5(String searchTerm, int termLimit) {
    System.out.println ("*************************************************************************");
    System.out.println ("***************************  Execute Query 5  ***************************");
    System.out.println ("*************************************************************************");
    
    final String searchFor = searchTerm.toLowerCase();
    
    //Open our dataset
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
    
    Dataset<Row> results = sparkSession.sql("SELECT userName, statusID, createdDate FROM tweets " + 
        "WHERE LOWER(filteredText) LIKE '%" + searchFor + "%' ORDER BY createdDate desc");
    
    List<Row> topX = results.takeAsList(termLimit);
    List<TwitterStatusTopX> searchResults = new ArrayList<TwitterStatusTopX>(topX.size());
    for (Row row : topX) {
      TwitterStatusTopX tstx = new TwitterStatusTopX();
      tstx.setUserName(row.getString(0));
      tstx.setStatusID(row.getLong(1));
      tstx.setCreatedDate(row.getString(2));
      
      /**********************************************************************************
      //This is where we make our External API call to pull in additional data
      TwitterStatusExtras extras = TwitterCall.getTweet(tstx.getStatusID());
      tstx.setStatusText(extras.getStatusText());
      **********************************************************************************/
      
      //Instead, create the dynamic widget which will be used to render the tweet block
      tstx.setStatusText("https://publish.twitter.com/oembed?url=https://twitter.com/" + tstx.getUserName() + 
          "/status/" + tstx.getStatusID());
      
      searchResults.add(tstx);
    }
    
    //Since the dynamicPath can't have spaces, replace any that might be in the search term
    //with an underscore
    String altSearchTerm = searchTerm.toLowerCase().replaceAll(" ", "_");
    
    String dynamicPath = "/proj3/query5/" + altSearchTerm + "/" + termLimit;
    try {
      Configuration hdfsConfiguration = new Configuration();
      hdfsConfiguration.set("fs.defaultFS", "hdfs://localhost:9000");
      FileSystem hdfs                 = FileSystem.get(hdfsConfiguration);
      
      Path checkFile = new Path(dynamicPath);
      if (hdfs.exists(checkFile)) {
        System.out.println ("I need to purge before writing!");
        hdfs.delete(checkFile, true);
      } 
    } catch (IOException e) {
      e.printStackTrace();
    }

    String outputPath = "hdfs://localhost:9000" + dynamicPath;
    JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());
    JavaRDD<TwitterStatusTopX> resultRDD = context.parallelize(searchResults);
    resultRDD.saveAsTextFile(outputPath);
    
    System.out.println ("Query Results Written to: " + outputPath);
    
    System.out.println ("-------------------------------------------------------------------------");
    System.out.println ("-----------------------------  End Query 5  -----------------------------");
    System.out.println ("-------------------------------------------------------------------------");
    
    return "{}";
  }
}

package com.ac.umkc.spark.util;

import oauth.signpost.OAuthConsumer;
import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import com.ac.umkc.spark.data.TwitterStatusExtras;

/**
 * @author AC010168
 *
 * This class just helps us execute the Twitter call.  We're having to use Deprecated
 * Libraries, because the version of hadoop imported from spark is using 3.x versions of
 * Apache's httpclient libraries, and we want to be using the 4.x versions.
 * 
 * The only way to resolve this dependency is to use the older, deprecated classes to
 * facilitate our HTTPClient work.
 */
@SuppressWarnings("deprecation")
public class TwitterCall {

  /** OAuth Information for apshaiTerp Developer account on Twitter */
  private static final String ACCESS_TOKEN        = "239401078-eDjOFRN4gICrqcNFyZIojk260ektMEXo3whHBs3v";
  /** OAuth Information for apshaiTerp Developer account on Twitter */
  private static final String ACCESS_TOKEN_SECRET = "3T2JlTzARHylyslH6k4O8G35oRXll0YMXnkSgqqfkjJ9n";
  /** OAuth Information for apshaiTerp Developer account on Twitter */
  private static final String CONSUMER_KEY        = "rzdr518OzoEWmGbD0yie3yDfb";
  /** OAuth Information for apshaiTerp Developer account on Twitter */
  private static final String CONSUMER_SECRET     = "7AFOzobLLXLviMYaMhrXH2oLGlntW0G6tCHJi4LiddWh6AuDBn";
  
  /**
   * Helper method to handle returning the raw tweet text from a single tweet.
   * 
   * @param statusID the id for the tweet we need to fetch
   * 
   * @return A valid object with our extra status information in it, or null.
   */
  @SuppressWarnings("resource")
  public static TwitterStatusExtras getTweet(long statusID) {
    HttpClient   client   = null;
    HttpResponse response = null;
    
    String twitterURL          = null;
    String responseString      = null;
    TwitterStatusExtras status = null;

    try {
      OAuthConsumer consumer = new CommonsHttpOAuthConsumer(CONSUMER_KEY, CONSUMER_SECRET);
      consumer.setTokenWithSecret(ACCESS_TOKEN, ACCESS_TOKEN_SECRET);
      
      client = new DefaultHttpClient();
      
      twitterURL = "https://api.twitter.com/1.1/statuses/lookup.json?id=" + statusID;
      
      HttpGet request = new HttpGet(twitterURL);
      consumer.sign(request);
      response = client.execute(request);
      
      HttpEntity entity = response.getEntity();
      responseString = EntityUtils.toString(entity);
      
      try {
        JSONObject errors = new JSONObject(responseString);
        if (errors.has("errors")) {
          System.out.println (twitterURL);
          System.out.println (responseString);
          System.out.println ("I hit my tweet request limit and cannot continue...");
          return null;
        }
        if (errors.has("error")) {
          System.out.println (twitterURL);
          System.out.println (responseString);
          System.out.println ("I do not have persmission to get this data...");
          return null;
        }
      } catch (Throwable t) { /** Ignore Me */ }

      JSONArray allTweets = null;
      try {
        allTweets = new JSONArray(responseString);
      } catch (Throwable t) {
        System.out.println (twitterURL);
        System.out.println (responseString);
      
        System.out.println ("There was something wrong with the response.  Re-attempting call");
        //Beware, this is unbounded recursion
        return getTweet(statusID);
      }
      
      JSONObject singleTweet = allTweets.getJSONObject(0);
      status = new TwitterStatusExtras();
      
      status.setStatusID(singleTweet.getLong("id"));
      status.setStatusText(singleTweet.getString("text"));
      
      return status;
    } catch (Throwable t) {
      System.out.println(twitterURL);
      System.err.println(responseString);
      System.err.println("Something bad happened parsing tweet for statusID " + statusID + "!");
      t.printStackTrace();
      return null;
    }
  }
}

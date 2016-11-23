package com.ac.umkc.spark.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import com.ac.umkc.spark.data.GoogleData;

/**
 * @author AC010168
 *
 */
@SuppressWarnings("deprecation")
public class GoogleCall {
  
  /** Google Developer API Key required for submitting call */
  private static final String API_KEY = "AIzaSyB0OPbkLdr319NnQll7B_RT8pyPYhahHqQ";
  
  /**
   * Helper method to run the provided location through the Google API
   * @param location
   * @return
   */
  @SuppressWarnings("resource") 
  public static GoogleData getGoogleLocation(String location) {
    HttpClient   client   = null;
    HttpResponse response = null;
    
    String googleURL      = null;
    String responseString = null;
    GoogleData data       = null;
    
    //We first need to sanitize our data
    String saneLocation = location.replaceAll("%", "%25") ;
    
    saneLocation = saneLocation.replaceAll(" ", "%20");
    saneLocation = saneLocation.replaceAll(";", "%3B");
    saneLocation = saneLocation.replaceAll("/", "%2F");
    saneLocation = saneLocation.replaceAll("\\?", "%3F");
    saneLocation = saneLocation.replaceAll(":", "%3A");
    saneLocation = saneLocation.replaceAll("@", "%40");
    saneLocation = saneLocation.replaceAll("=", "%3D");
    saneLocation = saneLocation.replaceAll("&", "%26");
    
    saneLocation = saneLocation.replaceAll("\"", "%22");
    saneLocation = saneLocation.replaceAll("<", "%3C");
    saneLocation = saneLocation.replaceAll(">", "%3E");
    saneLocation = saneLocation.replaceAll("#", "%23");
    saneLocation = saneLocation.replaceAll("\\{", "%7B");
    saneLocation = saneLocation.replaceAll("\\}", "%7D");
    saneLocation = saneLocation.replaceAll("\\|", "%7C");
    saneLocation = saneLocation.replaceAll("\\\\", "%5C");
    saneLocation = saneLocation.replaceAll("\\^", "%5E");
    saneLocation = saneLocation.replaceAll("~", "%7E");
    saneLocation = saneLocation.replaceAll("\\[", "%5B");
    saneLocation = saneLocation.replaceAll("\\]", "%5D");
    saneLocation = saneLocation.replaceAll("`", "%60");

    try {
      client = new DefaultHttpClient();
      
      googleURL = "https://maps.googleapis.com/maps/api/geocode/json?address=" + saneLocation + 
          "&key=" + API_KEY;

      HttpGet request = new HttpGet(googleURL);
      response = client.execute(request);

      HttpEntity entity = response.getEntity();
      responseString = EntityUtils.toString(entity);
      
      /********************************************************
      Key Fields
      { "results" : [
           "formatted_address" : <String>
           "location" : {
               "lat" : -36.8484597,
               "lng" : 174.7633315
           }
        "status" : "OK"
      }
      Bad Response
      {
         "results" : [],
         "status" : "ZERO_RESULTS"
      }
      ********************************************************/
      JSONObject baseReply = new JSONObject(responseString);
      String status        = baseReply.getString("status");
      
      if (status.equalsIgnoreCase("ZERO_RESULTS")) {
        //We found no results:
        System.out.println ("I could not find results for location " + location);
      } else if (status.equalsIgnoreCase("OK")) {
        data = new GoogleData();
        JSONObject results = baseReply.getJSONArray("results").getJSONObject(0);
        
        data.setLocation(results.getString("formatted_address"));
        
        JSONObject locationJSON = results.getJSONObject("geometry").getJSONObject("location");
        
        data.setGeoLat(locationJSON.getDouble("lat"));
        data.setGeoLon(locationJSON.getDouble("lng"));
        
        return data;
      } else {
        //We got something crazy:
        System.out.println ("I was processing location " + location);
        System.out.println ("We experienced an unknown status: " + status);
        System.out.println ("Google URL: " + googleURL);
        System.out.println ("Response String: " + responseString);
        
        return null;
      }
    } catch (Throwable t) {
      System.out.println ("There was a problem: " + t.getMessage());
      System.out.println ("Google URL: " + googleURL);
      System.out.println ("Response String: " + responseString);
      t.printStackTrace();
    }
    
    return null;
  }

}

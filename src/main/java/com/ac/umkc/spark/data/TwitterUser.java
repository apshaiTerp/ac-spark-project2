package com.ac.umkc.spark.data;

import java.io.Serializable;

import org.json.JSONObject;

/**
 * Java POJO representing the User object from twitter, or at least the important fields we
 * want to capture.
 * 
 * @author AC010168
 *
 */
public class TwitterUser implements Serializable {

  /** Required ID for serialization */
  private static final long serialVersionUID = 8542981525843929901L;
  
  /** The ID for this twitter user */
  private long twitterID;
  /** The screenName for this twitter user */
  private String userName;
  /** The screenName for this twitter user */
  private String screenName;
  /** The number of followers this user has */
  private int followersCount;
  /** The number of friends (people they follow) this user has */
  private int friendsCount;
  /** The number of statuses (tweets) this user has posted */
  private int statusesCount;
  /** Custom value assigned to track user types */
  private String userType;
  /** The location this user is found at, if available */
  private String location;
  
  /**
   * Basic Constructor
   */
  public TwitterUser() {
    twitterID      = -1;
    userName       = null;
    screenName     = null;
    followersCount = -1;
    friendsCount   = -1;
    statusesCount  = -1;
    userType       = null;
    location       = null;
  }
  
  /**
   * Helper method to convert object to json string
   * @return A JSON-formatted string representing this object
   */
  public String jsonify() {
    return "{\"twitterID\":" + twitterID + ",\"userName\":\"" + userName + "\",\"screenName\":\"" + 
        screenName + "\",\"followersCount\":" + followersCount + ",\"friendsCount\":" + friendsCount + 
        ",\"statusesCount\":" + statusesCount + ",\"userType\":\"" + userType + "\",\"location\":\"" + 
        location + "\"}";
  }
  
  /**
   * Utility method to parse the JSON string into an object
   * 
   * @param line the json content to be parsed
   */
  public void parseFromJSON(String line) {
    //if (!line.endsWith("\"}"))
    //  line += "\"}";
    
    try {
      JSONObject jsonUser = new JSONObject(line);
      twitterID      = jsonUser.getLong("twitterID");
      try { userName = jsonUser.getString("userName"); } catch (Throwable t) { userName = "Not Parseable"; }
      screenName     = jsonUser.getString("screenName");
      followersCount = jsonUser.getInt("followersCount");
      friendsCount   = jsonUser.getInt("friendsCount");
      statusesCount  = jsonUser.getInt("statusesCount");
      userType       = jsonUser.getString("userType");
      try { location = jsonUser.getString("location").trim(); } catch (Throwable t) { location = "Not Parseable"; }
       
      //System.out.println ("Successfully Parsed");
    } catch (Throwable t) {
      System.out.println("UNABLE TO PARSE: [" + line + "]");
    }
  }
  
  /*
   * (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return jsonify();
  }

  /**
   * @return the twitterID
   */
  public long getTwitterID() {
    return twitterID;
  }

  /**
   * @param twitterID the twitterID to set
   */
  public void setTwitterID(long twitterID) {
    this.twitterID = twitterID;
  }

  /**
   * @return the userName
   */
  public String getUserName() {
    return userName;
  }

  /**
   * @param userName the userName to set
   */
  public void setUserName(String userName) {
    this.userName = userName.trim();
  }

  /**
   * @return the screenName
   */
  public String getScreenName() {
    return screenName;
  }

  /**
   * @param screenName the screenName to set
   */
  public void setScreenName(String screenName) {
    this.screenName = screenName.trim();
  }

  /**
   * @return the followersCount
   */
  public int getFollowersCount() {
    return followersCount;
  }

  /**
   * @param followersCount the followersCount to set
   */
  public void setFollowersCount(int followersCount) {
    this.followersCount = followersCount;
  }

  /**
   * @return the friendsCount
   */
  public int getFriendsCount() {
    return friendsCount;
  }

  /**
   * @param friendsCount the friendsCount to set
   */
  public void setFriendsCount(int friendsCount) {
    this.friendsCount = friendsCount;
  }

  /**
   * @return the statusesCount
   */
  public int getStatusesCount() {
    return statusesCount;
  }

  /**
   * @param statusesCount the statusesCount to set
   */
  public void setStatusesCount(int statusesCount) {
    this.statusesCount = statusesCount;
  }

  /**
   * @return the userType
   */
  public String getUserType() {
    return userType;
  }

  /**
   * @param userType the userType to set
   */
  public void setUserType(String userType) {
    this.userType = userType;
  }

  /**
   * @return the location
   */
  public String getLocation() {
    return location;
  }

  /**
   * @param location the location to set
   */
  public void setLocation(String location) {
    this.location = location.trim();
  }
}

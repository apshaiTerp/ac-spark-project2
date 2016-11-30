package com.ac.umkc.spark.data;

import scala.Serializable;

/**
 * Java POJO to represent the results of one of our queries to combine Spark results
 * with additional Twitter information.
 * 
 * @author AC010168
 *
 */
public class TwitterStatusTopX implements Serializable {

  /** Adding because it needs it */
  private static final long serialVersionUID = 4737010945082679500L;
  
  /** The unique ID for this tweet */
  private long statusID;
  /** The userName who wrote this tweet */
  private String userName;
  /** The date this tweet was tweeted */
  private String createdDate;
  /** The unfiltered text from this tweet */
  private String statusText;
  
  /**
   * Basic Constructor
   */
  public TwitterStatusTopX() {
    statusID    = -1;
    userName    = null;
    createdDate = null;
    statusText  = null;
  }

  @Override
  public String toString() {
    return "{\"userName\":\"" + userName + "\", \"statusID\":" + statusID + "\", \"createdDate\":\"" + createdDate + 
        "\", \"statusText\":" + statusText+ "\"}";
  }
  
  /**
   * @return the statusID
   */
  public long getStatusID() {
    return statusID;
  }

  /**
   * @param statusID the statusID to set
   */
  public void setStatusID(long statusID) {
    this.statusID = statusID;
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
    this.userName = userName;
  }

  /**
   * @return the createdDate
   */
  public String getCreatedDate() {
    return createdDate;
  }

  /**
   * @param createdDate the createdDate to set
   */
  public void setCreatedDate(String createdDate) {
    this.createdDate = createdDate;
  }

  /**
   * @return the statusText
   */
  public String getStatusText() {
    return statusText;
  }

  /**
   * @param statusText the statusText to set
   */
  public void setStatusText(String statusText) {
    this.statusText = statusText;
  }
}

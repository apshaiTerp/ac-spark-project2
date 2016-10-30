package com.ac.umkc.spark.data;

/**
 * Java POJO to represent the results of one of our queries to combine Spark results
 * with additional Twitter information.
 * 
 * @author AC010168
 *
 */
public class TwitterStatusTopX {

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
    if (statusText == null)
      this.statusText = null;
    else {
      this.statusText = statusText.trim();
      this.statusText = this.statusText.replace("\\", "\\\\");
      this.statusText = this.statusText.replace("\"", "\\\"");
      this.statusText = this.statusText.replace("/", "\\/");
    }
  }
}

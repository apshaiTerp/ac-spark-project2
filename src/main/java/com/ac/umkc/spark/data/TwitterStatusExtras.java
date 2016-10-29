package com.ac.umkc.spark.data;

/**
 * @author AC010168
 *
 */
public class TwitterStatusExtras {
  
  /** The unique ID for this tweet */
  private long statusID;
  /** The unfiltered text from this tweet */
  private String statusText;
  
  /**
   * Basic Constructor
   */
  public TwitterStatusExtras() {
    statusID   = -1;
    statusText = null;
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

package com.ac.umkc.spark.data;

import scala.Serializable;

/**
 * @author AC010168
 *
 */
public class TweetsDayData implements Serializable {

  /** Added because it needs it */
  private static final long serialVersionUID = -13464562351351L;

  private String userType;
  private String shortDate;
  private int    year;
  private int    month;
  private int    day;
  private int    count;
  
  public TweetsDayData() {
    userType  = null;
    shortDate = null;
    year      = 0;
    month     = 0;
    day       = 0;
    count     = 0;
  }

  @Override
  public String toString() {
    return "{\"userType\":\"" + userType + "\",\"shortDate\":\"" + shortDate + "\",\"count\":" + count + 
        ",\"year\":" + year + ",\"month\":" + month + ",\"day\":" + day + "}";
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
   * @return the shortDate
   */
  public String getShortDate() {
    return shortDate;
  }

  /**
   * @param shortDate the shortDate to set
   */
  public void setShortDate(String shortDate) {
    this.shortDate = shortDate;
  }

  /**
   * @return the year
   */
  public int getYear() {
    return year;
  }

  /**
   * @param year the year to set
   */
  public void setYear(int year) {
    this.year = year;
  }

  /**
   * @return the month
   */
  public int getMonth() {
    return month;
  }

  /**
   * @param month the month to set
   */
  public void setMonth(int month) {
    this.month = month;
  }

  /**
   * @return the day
   */
  public int getDay() {
    return day;
  }

  /**
   * @param day the day to set
   */
  public void setDay(int day) {
    this.day = day;
  }

  /**
   * @return the count
   */
  public int getCount() {
    return count;
  }

  /**
   * @param count the count to set
   */
  public void setCount(int count) {
    this.count = count;
  }
  
}

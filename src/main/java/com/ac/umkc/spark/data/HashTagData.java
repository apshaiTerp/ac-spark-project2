package com.ac.umkc.spark.data;

import scala.Serializable;

/**
 * @author AC010168
 *
 */
public class HashTagData implements Serializable {

  /** Added because it needs it */
  private static final long serialVersionUID = -4452757429427453478L;

  private String hashTag;
  private int    count;
  
  public HashTagData() {
    hashTag = null;
    count   = 0;
  }
  
  public HashTagData(String hashTag, int count) {
    this.hashTag = hashTag;
    this.count   = count;
  }
  
  @Override
  public String toString() {
    return "{\"hashTag\":\"" + hashTag + "\", \"count\":" + count + "}";
  }

  /**
   * @return the hashTag
   */
  public String getHashTag() {
    return hashTag;
  }

  /**
   * @param hashTag the hashTag to set
   */
  public void setHashTag(String hashTag) {
    this.hashTag = hashTag;
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

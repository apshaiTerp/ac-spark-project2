package com.ac.umkc.spark.data;

import scala.Serializable;

/**
 * @author AC010168
 *
 */
public class GoogleData implements Serializable {
  
  /** Adding so I can serialize this mess */
  private static final long serialVersionUID = 336972756824513853L;
  
  private String location;
  private double geoLat;
  private double geoLon;
  
  public GoogleData() {
    location = null;
    geoLat = 0.0;
    geoLon = 0.0;
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
    this.location = location;
  }

  /**
   * @return the geoLat
   */
  public double getGeoLat() {
    return geoLat;
  }

  /**
   * @param geoLat the geoLat to set
   */
  public void setGeoLat(double geoLat) {
    this.geoLat = geoLat;
  }

  /**
   * @return the geoLon
   */
  public double getGeoLon() {
    return geoLon;
  }

  /**
   * @param geoLon the geoLon to set
   */
  public void setGeoLon(double geoLon) {
    this.geoLon = geoLon;
  }
}

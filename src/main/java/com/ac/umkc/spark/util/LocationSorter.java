package com.ac.umkc.spark.util;

import java.util.Comparator;

import scala.Serializable;
import scala.Tuple2;

/**
 * @author AC010168
 *
 */
public class LocationSorter implements Comparator<Tuple2<String, Integer>>, Serializable {

  /** Adding because it wants it */
  private static final long serialVersionUID = 1842363348761289758L;

  public int compare(Tuple2<String, Integer> arg0, Tuple2<String, Integer> arg1) {
    return arg0._2().compareTo(arg1._2());
  }
}

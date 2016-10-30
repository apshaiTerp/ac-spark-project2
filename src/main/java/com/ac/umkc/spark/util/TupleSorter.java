package com.ac.umkc.spark.util;

import java.util.Comparator;

import scala.Serializable;
import scala.Tuple2;

/**
 * Helpful sorting utility for sorting Tuples by values instead of keys.
 * 
 * @author AC010168
 *
 */
public class TupleSorter implements Comparator<Tuple2<String, Integer>>, Serializable {

  /** Adding because it wants it */
  private static final long serialVersionUID = 1842363348761289758L;

  public int compare(Tuple2<String, Integer> arg0, Tuple2<String, Integer> arg1) {
    int partial = arg1._2().compareTo(arg0._2());
    if (partial == 0)
      return arg0._1().compareToIgnoreCase(arg1._1());
    return partial;
  }
}

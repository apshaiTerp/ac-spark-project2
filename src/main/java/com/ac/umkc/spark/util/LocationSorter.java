package com.ac.umkc.spark.util;

import java.util.Comparator;

import scala.Tuple2;

/**
 * @author AC010168
 *
 */
public class LocationSorter implements Comparator<Tuple2<String, Integer>> {

  public int compare(Tuple2<String, Integer> arg0, Tuple2<String, Integer> arg1) {
    return arg0._2().compareTo(arg1._2());
  }
}

package com.ac.umkc.spark.util;

import java.util.Comparator;

import com.ac.umkc.spark.data.GoogleData;

import scala.Serializable;
import scala.Tuple2;

/**
 * @author AC010168
 *
 */
public class GoogleSorter implements Comparator<Tuple2<GoogleData, Integer>>, Serializable {

  /** Adding because it wants it */
  private static final long serialVersionUID = 6920513545421301653L;

  public int compare(Tuple2<GoogleData, Integer> arg0, Tuple2<GoogleData, Integer> arg1) {
    int partial = arg1._2().compareTo(arg0._2());
    if (partial == 0)
      return arg0._1().getLocation().compareToIgnoreCase(arg1._1().getLocation());
    return partial;
  }
}

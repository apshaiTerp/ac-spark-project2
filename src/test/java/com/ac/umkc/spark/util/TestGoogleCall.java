package com.ac.umkc.spark.util;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Date;

import org.junit.Test;

import com.ac.umkc.spark.data.GoogleData;

/**
 * @author AC010168
 *
 */
public class TestGoogleCall {

  @Test
  public void testGoogleCalls() {
    try {
      System.out.println ("Input Location:  " + "London, UK");
      GoogleData data = GoogleCall.getGoogleLocation("London, UK");
      System.out.println ("Output Location: " + data.getLocation());
      
      System.out.println ("Input Location:  " + "Radlett, UK | Krakow,Poland");
      data = GoogleCall.getGoogleLocation("Radlett, UK | Krakow,Poland");
      System.out.println ("Output Location: " + data.getLocation());
      
      System.out.println ("Input Location:  " + "@Central #03-81 JCube #04-06");
      data = GoogleCall.getGoogleLocation("@Central #03-81 JCube #04-06");
      System.out.println ("Output Location is null: " + (data == null));
      
      assertTrue("The world is ending", true);
    } catch (Throwable t) {
      t.printStackTrace();
      fail("I didn't work: " + t.getMessage());
    }
  }
}

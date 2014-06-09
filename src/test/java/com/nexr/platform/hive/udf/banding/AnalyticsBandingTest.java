package com.nexr.platform.hive.udf.banding;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * User: kaushik Date: 09/06/14 Time: 8:08 PM
 */
public class AnalyticsBandingTest
{

  private AnalyticsBanding analyticsBanding;

  @Before
  public void setup()
  {
    analyticsBanding = new AnalyticsBanding();
  }

  @Test
  public void testBanding()
  {

    Map<String, String> map = sampleBandDef();

    Assert.assertEquals(analyticsBanding.getBand(10, 100, map), "band5");
    Assert.assertEquals(analyticsBanding.getBand(20, 100, map), "band5");
    Assert.assertEquals(analyticsBanding.getBand(30, 100, map), "band4");
    Assert.assertEquals(analyticsBanding.getBand(50, 100, map), "band3");
    Assert.assertEquals(analyticsBanding.getBand(70, 100, map), "band2");
    Assert.assertEquals(analyticsBanding.getBand(90, 100, map), "band1");
  }

  @Test
  public void testBanding2()
  {

    Map<String, String> map = sampleBandDef();

    Assert.assertEquals("band5", analyticsBanding.getBand(50, 100, map));
    Assert.assertEquals("band4",analyticsBanding.getBand(80, 100, map));
    Assert.assertEquals("band3",analyticsBanding.getBand(95, 100, map));
    Assert.assertEquals("band2",analyticsBanding.getBand(100, 100, map));
  }


  private Map<String, String> sampleBandDef()
  {
    return new HashMap<String, String>()
      {
        {
          {
            put("band5", "0-20");
            put("band4", "21-40");
            put("band3", "41-60");
            put("band2", "61-80");
            put("band1", "81-100");
          }
        }
      };
  }

}

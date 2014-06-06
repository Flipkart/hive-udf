package com.nexr.platform.hive.udf.banding;

import junit.framework.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * User: kaushik Date: 06/06/14 Time: 6:46 PM
 */
public class SimpleBandingTest
{
  @Test
  public void testBanding()
  {
    SimpleBanding sb = new SimpleBanding();
    Map<String, String> map = new HashMap<String, String>()
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

    Assert.assertEquals(sb.getBand(10, 100, map), "band5");
    Assert.assertEquals(sb.getBand(20, 100, map), "band5");
    Assert.assertEquals(sb.getBand(55, 100, map), "band3");
    Assert.assertEquals(sb.getBand(80, 100, map), "band2");

    Assert.assertEquals(sb.getBand(-1, 100, map), SimpleBanding.NOBAND);
    Assert.assertEquals(sb.getBand(101, 100, map), SimpleBanding.NOBAND);
  }
}

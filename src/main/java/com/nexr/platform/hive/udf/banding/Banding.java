package com.nexr.platform.hive.udf.banding;

import org.apache.commons.lang.math.DoubleRange;
import org.apache.commons.lang.math.Range;

import java.util.HashMap;
import java.util.Map;

/**
 * User: kaushik Date: 06/06/14 Time: 5:47 PM
 */
public abstract class Banding
{
  public abstract String getBand(Number value, Number totSum, Map<String, String> bandDefMap);

  public Map<String,Range> getBandPercentageRangeMap(Map<String, String> bandDefMap)
  {
    Map<String, Range> map = new HashMap<String, Range>();
    for (String bandName : bandDefMap.keySet())
    {
      map.put(bandName, getRangeFromString(bandDefMap.get(bandName)));
    }
    return map;
  }

  private Range getRangeFromString(String rangeStr)
  {
    String[] rangeArray = rangeStr.trim().split("-");
    double minPercentage = asDouble(rangeArray[0]);
    double maxPercentage = asDouble(rangeArray[1]);

    return new DoubleRange(minPercentage, maxPercentage);
  }

  private double asDouble(String s)
  {
    return Double.parseDouble(s.trim());
  }
}

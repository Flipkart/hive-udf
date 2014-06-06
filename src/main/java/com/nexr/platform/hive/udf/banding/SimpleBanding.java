package com.nexr.platform.hive.udf.banding;

import org.apache.commons.lang.math.Range;

import java.util.Map;

/**
 * User: kaushik Date: 06/06/14 Time: 5:47 PM
 */
public class SimpleBanding extends Banding
{

  public static final String NOBAND = "band0";

  @Override
  public String getBand(Number currentVal, Number totSum, Map<String, String> bandDefMap)
  {
    String band = NOBAND;
    Map<String, Range> percentageRangeMap = getBandPercentageRangeMap(bandDefMap);
    double currentContrPercentage = (currentVal.doubleValue() * 100 / totSum.doubleValue());

    for (Map.Entry<String, Range> rangeEntry : percentageRangeMap.entrySet())
    {

      if (rangeEntry.getValue().containsDouble(currentContrPercentage))
      {
        band = rangeEntry.getKey();
        break;
      }
    }

    System.out.println("perc: " + currentContrPercentage + " band " + band + " map "+ percentageRangeMap);
    return band;
  }
}

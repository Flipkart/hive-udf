package com.nexr.platform.hive.udf.banding;

import org.apache.commons.lang.math.Range;

import java.util.*;

/**
 * User: kaushik Date: 09/06/14 Time: 7:34 PM
 */
public class AnalyticsBanding extends Banding
{

  public static final String NOBAND = "NOBAND";
  private List<String> bandOrder;
  private Map<String, Boolean> bandOccupation = new HashMap<String, Boolean>();

  @Override
  public String getBand(Number currentVal, Number totSum, Map<String, String> bandDefMap)
  {
    String band = NOBAND;
    Map<String, Range> percentageRangeMap = getBandPercentageRangeMap(bandDefMap);
    setBandOrder(percentageRangeMap);
    double currentContrPercentage = (currentVal.doubleValue() * 100 / totSum.doubleValue());

    for (Map.Entry<String, Range> rangeEntry : percentageRangeMap.entrySet())
    {
      Range range = rangeEntry.getValue();
      if (range.containsDouble(currentContrPercentage))
      {
        band = rangeEntry.getKey();
        band = bandAfterShifting(band);
        if (band!= NOBAND && !bandOccupation.containsKey(band))
          bandOccupation.put(band, true);
        break;
      }
    }
    return band;
  }

  private String bandAfterShifting(String band)
  {
    String res = band;
    int index = bandOrder.indexOf(band);
    if (index != 0)
    {
      String prevBand = bandOrder.get(index - 1);
      if (bandOccupation.get(prevBand) == null)
      {
        return bandAfterShifting(prevBand);
      }
    }
    return res;
  }

  private void setBandOrder(Map<String, Range> percentageRangeMap)
  {
    if (bandOrder == null)
    {
      bandOrder = orderedBandMap(percentageRangeMap);
    }
  }

  private List<String> orderedBandMap(Map<String, Range> bandPercentageRangeMap)
  {

    Map<Double, String> rangeStartBandNameMap = new HashMap<Double, String>();
    Double[] rangeStarts = new Double[bandPercentageRangeMap.size()];
    int i = 0;
    for (String band : bandPercentageRangeMap.keySet())
    {
      double db = bandPercentageRangeMap.get(band).getMinimumDouble();
      rangeStarts[i++] = db;
      rangeStartBandNameMap.put(db, band);
    }
    Arrays.sort(rangeStarts);

    List<String> bandOrder = new ArrayList<String>();
    for (Double rangeStart : rangeStarts)
    {
      String band = rangeStartBandNameMap.get(rangeStart);
      bandOrder.add(band);
    }
    return bandOrder;
  }

}

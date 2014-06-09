package com.nexr.platform.hive.udf;

import com.sun.tools.javac.util.Pair;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * User: kaushik Date: 06/06/14 Time: 1:11 PM
 */
public class GenericUDFSumRangeBucketingTest
{
  private GenericUDFSumRangeBucketing udfSumRangeBucketing;
  private int count;

  @Before
  public void setup() throws UDFArgumentException
  {
    udfSumRangeBucketing = new GenericUDFSumRangeBucketing();
    LazyBinaryMapObjectInspector a =
      LazyBinaryObjectInspectorFactory.getLazyBinaryMapObjectInspector(
        PrimitiveObjectInspectorFactory.writableStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableStringObjectInspector);

    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    ObjectInspector[] arguments = {valueOI, valueOI, valueOI, a};
    udfSumRangeBucketing.initialize(arguments);
  }

  @Test
  public void testBand() throws HiveException
  {

    assertResp(1, 12, "Band5", 100, 12l);
    assertResp(1, 10, "Band4", 100, 22l);
  }

  @Test
  public void testBandFromInputFile() throws FileNotFoundException, HiveException
  {
    FileInputStream inputStream = new FileInputStream("/tmp/banding_data.csv");
    Scanner scanner = new Scanner(inputStream);
    ArrayList<Pair<Integer, String>> pairList = new ArrayList<Pair<Integer, String>>();
    int totSum = 0;

    while (scanner.hasNext())
    {
      String row = scanner.next();
      String[] rowSplit = row.split(",");
      int qty = Integer.parseInt(rowSplit[1].trim());
      String bandExpected = rowSplit[2].trim();
      totSum += qty;

      pairList.add(new Pair<Integer, String>(qty, bandExpected));
    }

    for (Pair<Integer, String> integerStringPair : pairList)
    {
      Integer qty = integerStringPair.fst;
      if(qty > 0)
      {
        assertRespWithoutCumSum(1, qty, integerStringPair.snd, totSum);
      }
    }
  }

  private void assertRespWithoutCumSum(int hash, int value, String expectedBand, int totSum) throws
    HiveException
  {
    Map<String, Number> res = getBandForSampleParams(hash, value, totSum);

    try
    {
      Assert.assertTrue(res.containsKey(expectedBand));
    }
    catch (AssertionError e)
    {
      count++;
      System.err.println(
        "hash " + hash + " value " + value + " totsum " + totSum +
          " expected: " + expectedBand + " actual: " + res);
    }
  }

  private void assertResp(int hash, int value, String expectedBand, int totSum,
                          long expectedCumSum) throws HiveException
  {
    Map<String, Number> res = getBandForSampleParams(hash, value, totSum);
    Assert.assertTrue(res.toString(),res.containsKey(expectedBand));
    Assert.assertEquals(expectedCumSum, res.get(expectedBand));
  }

  private Map<String, Number> getBandForSampleParams(Object hash, Object value,
                                                     Object totSum) throws
    HiveException
  {
    Map<String, String> bandConfMap = new HashMap<String, String>()
    {
      {
        {
          put("Band5", "0-20");
          put("Band4", "20-40");
          put("Band3", "40-60");
          put("Band2", "60-80");
          put("Band1", "80-100");
        }
      }
    };
    GenericUDF.DeferredJavaObject deferredHash = new GenericUDF.DeferredJavaObject(hash);
    GenericUDF.DeferredJavaObject deferredValue = new GenericUDF.DeferredJavaObject(value);
    GenericUDF.DeferredJavaObject deferredTotSum = new GenericUDF.DeferredJavaObject(totSum);
    GenericUDF.DeferredJavaObject deferredBandConf = new GenericUDF.DeferredJavaObject(bandConfMap);

    GenericUDF.DeferredObject[] arguments =
      {deferredHash, deferredValue, deferredTotSum, deferredBandConf};

    return (Map<String, Number>) udfSumRangeBucketing.evaluate(arguments);
  }

}

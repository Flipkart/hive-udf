package com.nexr.platform.hive.udf;

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

import java.util.HashMap;
import java.util.Map;

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
  public void test() throws HiveException
  {

    assertResp(1, 12, "band5", 100, 12l);
    assertResp(1, 10, "band4", 100, 22l);
  }

  @Test
  public void test2() throws HiveException
  {
    assertRespWithoutCumSum(1, 101, "band5", 1078);
    assertRespWithoutCumSum(1, 64, "band5", 1078);
    assertRespWithoutCumSum(1, 61, "band4", 1078);
    assertRespWithoutCumSum(1, 49, "band4", 1078);
    assertRespWithoutCumSum(1, 46, "band4", 1078);
    assertRespWithoutCumSum(1, 40, "band4", 1078);
    assertRespWithoutCumSum(1, 38, "band4", 1078);
    assertRespWithoutCumSum(1, 34, "band3", 1078);
    assertRespWithoutCumSum(1, 32, "band3", 1078);
    assertRespWithoutCumSum(1, 31, "band3", 1078);
    assertRespWithoutCumSum(1, 29, "band3", 1078);
    assertRespWithoutCumSum(1, 26, "band3", 1078);
    assertRespWithoutCumSum(1, 26, "band3", 1078);
    assertRespWithoutCumSum(1, 24, "band3", 1078);
    assertRespWithoutCumSum(1, 22, "band3", 1078);
    assertRespWithoutCumSum(1, 20, "band2", 1078);
    assertRespWithoutCumSum(1, 20, "band2", 1078);
    assertRespWithoutCumSum(1, 20, "band3", 1078);
    assertRespWithoutCumSum(1, 19, "band2", 1078);
    assertRespWithoutCumSum(1, 19, "band2", 1078);
    assertRespWithoutCumSum(1, 17, "band2", 1078);
    assertRespWithoutCumSum(1, 16, "band2", 1078);
    assertRespWithoutCumSum(1, 15, "band2", 1078);
    assertRespWithoutCumSum(1, 14, "band2", 1078);
    assertRespWithoutCumSum(1, 14, "band2", 1078);
    assertRespWithoutCumSum(1, 14, "band2", 1078);
    assertRespWithoutCumSum(1, 12, "band2", 1078);
    assertRespWithoutCumSum(1, 12, "band2", 1078);
    assertRespWithoutCumSum(1, 11, "band1", 1078);
    assertRespWithoutCumSum(1, 11, "band2", 1078);
    assertRespWithoutCumSum(1, 11, "band2", 1078);
    assertRespWithoutCumSum(1, 10, "band1", 1078);
    assertRespWithoutCumSum(1, 9, "band1", 1078);
    assertRespWithoutCumSum(1, 9, "band1", 1078);
    assertRespWithoutCumSum(1, 8, "band1", 1078);
    assertRespWithoutCumSum(1, 8, "band1", 1078);
    assertRespWithoutCumSum(1, 8, "band1", 1078);
    assertRespWithoutCumSum(1, 7, "band1", 1078);
    assertRespWithoutCumSum(1, 7, "band1", 1078);
    assertRespWithoutCumSum(1, 7, "band1", 1078);
    assertRespWithoutCumSum(1, 7, "band1", 1078);
    assertRespWithoutCumSum(1, 6, "band1", 1078);
    assertRespWithoutCumSum(1, 6, "band1", 1078);
    assertRespWithoutCumSum(1, 5, "band1", 1078);
    assertRespWithoutCumSum(1, 5, "band1", 1078);
    assertRespWithoutCumSum(1, 5, "band1", 1078);
    assertRespWithoutCumSum(1, 5, "band1", 1078);
    assertRespWithoutCumSum(1, 5, "band1", 1078);
    assertRespWithoutCumSum(1, 5, "band1", 1078);
    assertRespWithoutCumSum(1, 4, "band1", 1078);
    assertRespWithoutCumSum(1, 4, "band1", 1078);
    assertRespWithoutCumSum(1, 4, "band1", 1078);
    assertRespWithoutCumSum(1, 4, "band1", 1078);
    assertRespWithoutCumSum(1, 4, "band1", 1078);
    assertRespWithoutCumSum(1, 4, "band1", 1078);
    assertRespWithoutCumSum(1, 3, "band1", 1078);
    assertRespWithoutCumSum(1, 3, "band1", 1078);
    assertRespWithoutCumSum(1, 3, "band1", 1078);
    assertRespWithoutCumSum(1, 3, "band1", 1078);
    assertRespWithoutCumSum(1, 3, "band1", 1078);
    assertRespWithoutCumSum(1, 3, "band1", 1078);
    assertRespWithoutCumSum(1, 2, "band1", 1078);
    assertRespWithoutCumSum(1, 2, "band1", 1078);
    assertRespWithoutCumSum(1, 2, "band1", 1078);
    assertRespWithoutCumSum(1, 2, "band1", 1078);
    assertRespWithoutCumSum(1, 2, "band1", 1078);
    assertRespWithoutCumSum(1, 2, "band1", 1078);
    assertRespWithoutCumSum(1, 2, "band1", 1078);
    assertRespWithoutCumSum(1, 2, "band1", 1078);
    assertRespWithoutCumSum(1, 2, "band1", 1078);
    assertRespWithoutCumSum(1, 2, "band1", 1078);
    assertRespWithoutCumSum(1, 2, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
    assertRespWithoutCumSum(1, 1, "band1", 1078);
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
    System.out.println("mismatch: " + count);
  }

  private void assertResp(int hash, int value, String expectedBand, int totSum,
                          long expectedCumSum) throws HiveException
  {
    Map<String, Number> res = getBandForSampleParams(hash, value, totSum);
    Assert.assertTrue(res.containsKey(expectedBand));
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
          put("band5", "0-20");
          put("band4", "20-40");
          put("band3", "40-60");
          put("band2", "60-80");
          put("band1", "80-100");
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

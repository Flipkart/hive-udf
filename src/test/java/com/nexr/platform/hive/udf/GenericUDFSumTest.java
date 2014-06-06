package com.nexr.platform.hive.udf;

import com.nexr.platform.hive.udf.GenericUDFSum;
import junit.framework.Assert;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;

/**
 * User: kaushik Date: 05/06/14 Time: 11:26 PM
 */
public class GenericUDFSumTest
{

  private GenericUDFSum genericUDFSum;

  @Before
  public void setup() throws UDFArgumentException
  {
    genericUDFSum = new GenericUDFSum();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    ObjectInspector[] arguments = {valueOI, valueOI};
    genericUDFSum.initialize(arguments);
  }

  @Test
  public void testCumulativeSumForSequenceOfSameGroupNumbers() throws HiveException
  {
    long res1 = getCumulativeSum(genericUDFSum, 1, 100);
    long res2 = getCumulativeSum(genericUDFSum, 1, 30);
    long res3 = getCumulativeSum(genericUDFSum, 2, 200);
    long res4 = getCumulativeSum(genericUDFSum, 2, 50);

    Assert.assertEquals(100l, res1);
    Assert.assertEquals(130l, res2);
    Assert.assertEquals(200l, res3);
    Assert.assertEquals(250l, res4);
  }

  @Test
  public void testCumulativeSumRefreshForNonContiguousOccuranceOfSameGroupNumbers() throws HiveException
  {
    long res1 = getCumulativeSum(genericUDFSum, 1, 100);
    long res2 = getCumulativeSum(genericUDFSum, 2, 200);
    long res3 = getCumulativeSum(genericUDFSum, 1, 30);
    long res4 = getCumulativeSum(genericUDFSum, 2, 50);

    Assert.assertEquals(100l, res1);
    Assert.assertEquals(200l, res2);
    Assert.assertEquals(30l, res3);
    Assert.assertEquals(50l, res4);
  }

  private long getCumulativeSum(GenericUDFSum genericUDFSum, int hash, int value1) throws HiveException
  {
    DeferredJavaObject deferredHash = new DeferredJavaObject(hash);
    DeferredJavaObject deferredValue = new DeferredJavaObject(value1);
    DeferredObject[] arguments = {deferredHash, deferredValue};
    return ((LongWritable) genericUDFSum.evaluate(arguments)).get();
  }
}

package com.nexr.platform.hive.udf;

import com.sun.tools.javac.util.Pair;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;

/**
 * User: kaushik Date: 06/06/14 Time: 1:11 PM
 */
public class GenericUDFSumRangeBucketingTest
{
  private int mismatchCount;
  private GenericUDFSumRangeBucketing udfSumRangeBucketing;

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

    assertResp(1, 12, "Band5", 100);
    assertResp(1, 10, "Band4", 100);
  }

  @Test
  public void testBandFromInputFile() throws IOException, HiveException
  {
    FileInputStream inputStream = new FileInputStream("/tmp/sales_band_with_b0.csv");
    Scanner scanner = new Scanner(inputStream);
    List<List<Object>> debugData = new ArrayList<List<Object>>();
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
      if (qty > 0)
      {
        assertRespWithoutCumSum(1, qty, integerStringPair.snd, totSum, debugData);
      }
    }
    System.out.println("Tot mismatch " + mismatchCount);

    writeDebug(debugData);
  }

  private void writeDebug(List<List<Object>> debugData) throws IOException
  {
    FileWriter fileWriter = new FileWriter("/tmp/debug_banding.csv");
    fileWriter.write("Quantity,TotalSum,CumulativeSum,Cumulative%Contribution,Band,MismatchErrorExplanation" + "\n");
    for (List<Object> objects : debugData)
    {
      fileWriter.write(StringUtils.join(objects, ",") + "\n");
    }
    fileWriter.close();
  }

  private void assertRespWithoutCumSum(int hash, int value, String expectedBand, int totSum,
                                       List<List<Object>> debugData) throws
    HiveException
  {
    Number cumSum = 0;
    String actualBand = null;
    String percentage = null;
    DecimalFormat df = new DecimalFormat("#.##");

    try
    {
      DeferredObject[] arguments = getSampleParams(hash, value, totSum);
      Pair<String, Number> resp = udfSumRangeBucketing.getBandAndCumSum(arguments);
      cumSum = resp.snd;
      actualBand = resp.fst;
      percentage = df.format(cumSum.doubleValue() * 100.0 / totSum);
      Assert.assertEquals(expectedBand, actualBand);

      debugData
        .add(new ArrayList<Object>(Arrays.asList(value, totSum, cumSum, percentage, actualBand)));
    }
    catch (AssertionError e)
    {
      mismatchCount++;
      debugData.add(
        new ArrayList<Object>(
          Arrays.asList(value, totSum, cumSum, percentage, actualBand, e.getMessage())));
    }
  }

  private void assertResp(int hash, int value, String expectedBand, int totSum) throws HiveException
  {
    String actualBand = getBandForSampleParams(hash, value, totSum);
    Assert.assertEquals(expectedBand, actualBand);
  }

  private String getBandForSampleParams(Object hash, Object value,
                                        Object totSum) throws
    HiveException
  {
    DeferredObject[] arguments = getSampleParams(hash, value, totSum);
    return udfSumRangeBucketing.evaluate(arguments).toString();
  }

  private DeferredObject[] getSampleParams(Object hash, Object value, Object totSum)
  {
    Map<Text, Text> bandConfMap = new HashMap<Text, Text>()
    {
      {
        {
          put(t("Band5"), t("0-20"));
          put(t("Band4"), t("20-40"));
          put(t("Band3"), t("40-60"));
          put(t("Band2"), t("60-80"));
          put(t("Band1"), t("80-100"));
        }
      }
    };
    GenericUDF.DeferredJavaObject deferredHash = new GenericUDF.DeferredJavaObject((hash));
    GenericUDF.DeferredJavaObject deferredValue = new GenericUDF.DeferredJavaObject((value));
    GenericUDF.DeferredJavaObject deferredTotSum = new GenericUDF.DeferredJavaObject((totSum));
    GenericUDF.DeferredJavaObject deferredBandConf = new GenericUDF.DeferredJavaObject(bandConfMap);

    return new DeferredObject[]{deferredHash, deferredValue, deferredTotSum, deferredBandConf};
  }

  private Text t(String str)
  {
    return new Text(str);
  }
}

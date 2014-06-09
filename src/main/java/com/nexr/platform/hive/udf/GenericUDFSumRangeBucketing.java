package com.nexr.platform.hive.udf;

import com.nexr.platform.hive.udf.banding.SimpleBanding;
import com.nexr.platform.hive.udf.common.Utils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.nexr.platform.hive.udf.common.Utils.validateAndGetWritableNumericType;

/**
 * User: kaushik Date: 06/06/14 Time: 12:27 AM
 */

// Args - hash, value, totSum, bandMap
@UDFType(deterministic = false, stateful = true)
public class GenericUDFSumRangeBucketing extends GenericUDF
{
  private SimpleBanding bandingLogic;
  private GenericUDFSum genericUDFSum;
  private ObjectInspector totSumOI;
  private PrimitiveObjectInspector valueInspector;
  private StandardMapObjectInspector mapInspector;
  private Text resultKeyText = new Text();
  private LongWritable resultLongValueWritable = new LongWritable();
  private DoubleWritable resultDoubleValueWritable = new DoubleWritable();
  private MapWritable resultMapWritable = new MapWritable();

  private StandardMapObjectInspector resultInspector;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException
  {
    if (arguments.length != 4)
    {
      throw new UDFArgumentException("Exactly four argument is expected.");
    }

    Utils.validateTypeOfArg(arguments[2], 2, ObjectInspector.Category.PRIMITIVE);
    Utils.validateTypeOfArg(arguments[3], 3, ObjectInspector.Category.MAP);

    totSumOI = arguments[2];
    valueInspector = validateAndGetWritableNumericType(arguments[2].getTypeName());

    WritableStringObjectInspector strOI =
      PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    mapInspector = ObjectInspectorFactory.getStandardMapObjectInspector(strOI, strOI);

    WritableLongObjectInspector longOI =
      PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    resultInspector = ObjectInspectorFactory.getStandardMapObjectInspector(strOI, longOI);

    if (!(mapInspector.getMapKeyObjectInspector() instanceof StringObjectInspector && mapInspector
      .getMapValueObjectInspector() instanceof StringObjectInspector))
      throw new UDFArgumentException("key/val of Map should both be strings");

    bandingLogic = new SimpleBanding();
    genericUDFSum = new GenericUDFSum();
    genericUDFSum.initialize(Arrays.copyOfRange(arguments, 0, 2));

    return resultInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException
  {
    resultMapWritable.clear();

    Object totSumValueArg = arguments[2].get();
    Object bandConfMapArg = arguments[3].get();

    Object writableCumulativeSum = genericUDFSum.evaluate(Arrays.copyOfRange(arguments, 0, 2));
    Map<Text, Text> bandConfMapRaw = (Map<Text, Text>) mapInspector.getMap(bandConfMapArg);
    Map<String, String> bandConfMap = new HashMap<String, String>();
    for (Map.Entry<Text, Text> textTextEntry : bandConfMapRaw.entrySet())
    {
      bandConfMap.put(textTextEntry.getKey().toString(), textTextEntry.getValue().toString());
    }

    String band = null;
    Number cumulativeSum = null;
    ObjectInspectorConverters.Converter converter =
      ObjectInspectorConverters.getConverter(totSumOI, valueInspector);

    if (valueInspector.getTypeName() == Constants.DOUBLE_TYPE_NAME)
    {
      Double totSumVal =
        ((Number) valueInspector.getPrimitiveJavaObject(totSumValueArg)).doubleValue();
      cumulativeSum = ((DoubleWritable) writableCumulativeSum).get();
      band = bandingLogic.getBand(cumulativeSum, totSumVal, bandConfMap);
      resultDoubleValueWritable.set(cumulativeSum.doubleValue());
      setRes(band, resultDoubleValueWritable);

    }
    else
    {
      long totSumVal = ((LongWritable) converter.convert(totSumValueArg)).get();
      cumulativeSum = ((LongWritable) writableCumulativeSum).get();
      band = bandingLogic.getBand(cumulativeSum, totSumVal, bandConfMap);
      resultLongValueWritable.set(cumulativeSum.longValue());
      setRes(band, resultLongValueWritable);
    }
    return resultMapWritable;
  }

  private void setRes(String band, Writable value)
  {
    resultKeyText.set(band);
    resultMapWritable.put(resultKeyText, resultLongValueWritable);
  }

  @Override
  public String getDisplayString(String[] children)
  {
    return null;
  }
}

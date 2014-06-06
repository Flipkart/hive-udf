package com.nexr.platform.hive.udf;

import com.nexr.platform.hive.udf.banding.SimpleBanding;
import com.nexr.platform.hive.udf.common.Utils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.io.LongWritable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.nexr.platform.hive.udf.common.Utils.validateAndGetPrimitiveNumericType;

/**
 * User: kaushik Date: 06/06/14 Time: 12:27 AM
 */

// Args - hash, value, totSum, bandMap
@UDFType(deterministic = false, stateful = true)
public class GenericUDFSumRangeBucketing extends GenericUDF
{
  private GenericUDFSum genericUDFSum;
  private PrimitiveObjectInspector valueInspector;
  private StandardMapObjectInspector mapInspector;
  private SimpleBanding banding;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException
  {
    if (arguments.length != 4)
    {
      throw new UDFArgumentException("Exactly four argument is expected.");
    }

    Utils.validateTypeOfArg(arguments[2], 2, ObjectInspector.Category.PRIMITIVE);
    Utils.validateTypeOfArg(arguments[3], 3, ObjectInspector.Category.MAP);
    valueInspector = validateAndGetPrimitiveNumericType(arguments[2].getTypeName());

    WritableStringObjectInspector strOI =
      PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    mapInspector = ObjectInspectorFactory.getStandardMapObjectInspector(strOI, strOI);

    if (!(mapInspector.getMapKeyObjectInspector() instanceof StringObjectInspector && mapInspector
      .getMapValueObjectInspector() instanceof StringObjectInspector))
      throw new UDFArgumentException("key/val of Map should both be strings");

    banding = new SimpleBanding();
    genericUDFSum = new GenericUDFSum();
    genericUDFSum.initialize(Arrays.copyOfRange(arguments, 0, 2));

    return valueInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException
  {
    Object totSumValueArg = arguments[2].get();
    Object bandConfMapArg = arguments[3].get();

    Object writableCumulativeSum = genericUDFSum.evaluate(Arrays.copyOfRange(arguments, 0, 2));
    Map<String, String> bandConfMag = (Map<String, String>) mapInspector.getMap(bandConfMapArg);

    String band = null;
    Number cumulativeSum = null;
    if (valueInspector.getTypeName() == Constants.DOUBLE_TYPE_NAME)
    {
      Double totSumVal =
        ((Number) valueInspector.getPrimitiveJavaObject(totSumValueArg)).doubleValue();
      cumulativeSum = ((DoubleWritable) writableCumulativeSum).get();
      band = banding.getBand(cumulativeSum, totSumVal, bandConfMag);
    }
    else
    {
      Long totSumVal = ((Number) valueInspector.getPrimitiveJavaObject(totSumValueArg)).longValue();
      cumulativeSum = ((LongWritable) writableCumulativeSum).get();
      band = banding.getBand(cumulativeSum, totSumVal, bandConfMag);
    }

    Map<String, Number> result = new HashMap<String, Number>();
    result.put(band, cumulativeSum);

    return result;
  }

  @Override
  public String getDisplayString(String[] children)
  {
    return null;
  }
}

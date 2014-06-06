package com.nexr.platform.hive.udf.common;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveWritableObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * User: kaushik Date: 06/06/14 Time: 1:23 AM
 */
public class Utils
{
  public static AbstractPrimitiveWritableObjectInspector validateAndGetWritableNumericType(String t) throws
    UDFArgumentTypeException
  {
    if (t.equals(Constants.TINYINT_TYPE_NAME)||
        t.equals(Constants.SMALLINT_TYPE_NAME)||
        t.equals(Constants.INT_TYPE_NAME)||
        t.equals(Constants.BIGINT_TYPE_NAME)) {
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    } else if (t.equals(Constants.FLOAT_TYPE_NAME)||
        t.equals(Constants.DOUBLE_TYPE_NAME)||
        t.equals(Constants.STRING_TYPE_NAME)) {
      return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    } else{
      throw new UDFArgumentTypeException(1,
          "Only numeric or string type arguments are accepted but "
          + t + " is passed.");
    }
  }


  public static AbstractPrimitiveJavaObjectInspector validateAndGetPrimitiveNumericType(String t) throws
    UDFArgumentTypeException
  {
    if (t.equals(Constants.TINYINT_TYPE_NAME)||
      t.equals(Constants.SMALLINT_TYPE_NAME)||
      t.equals(Constants.INT_TYPE_NAME)||
      t.equals(Constants.BIGINT_TYPE_NAME)) {
      return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    } else if (t.equals(Constants.FLOAT_TYPE_NAME)||
      t.equals(Constants.DOUBLE_TYPE_NAME)||
      t.equals(Constants.STRING_TYPE_NAME)) {
      return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    } else{
      throw new UDFArgumentTypeException(1,
        "Only numeric or string type arguments are accepted but "
          + t + " is passed.");
    }
  }

  public static void validateTypeOfArg(ObjectInspector argument, int arg_index,
                                 ObjectInspector.Category type) throws
    UDFArgumentTypeException
  {
    if (argument.getCategory() != type)
    {
      throw new UDFArgumentTypeException(arg_index,
        "Third argument must be " + type + ", but "
          + argument.getTypeName() + " is passed.");
    }
  }
}

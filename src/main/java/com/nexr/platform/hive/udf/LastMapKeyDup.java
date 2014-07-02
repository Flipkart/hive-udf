package com.nexr.platform.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMapKeys;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * User: kaushik Date: 24/06/14 Time: 11:18 PM
 */
public class LastMapKeyDup extends GenericUDFMapKeys
{
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException
  {
    return super.initialize(arguments);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException
  {
    return super.evaluate(arguments);
  }
}

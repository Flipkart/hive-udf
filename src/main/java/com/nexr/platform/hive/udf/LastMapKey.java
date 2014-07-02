package com.nexr.platform.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMapKeys;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.ArrayList;

/**
 * User: kaushik Date: 24/06/14 Time: 11:18 PM
 */
public class LastMapKey extends GenericUDFMapKeys
{
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException
  {
    return super.initialize(arguments);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException
  {
    ArrayList<Object> evaluate = (ArrayList<Object>) super.evaluate(arguments);
    ArrayList<Object> res = new ArrayList<Object>();
    res.add(evaluate.get(evaluate.size()-1));
    System.out.println("size is :" + res.size() + " obj is " + res );
    return res;
  }
}

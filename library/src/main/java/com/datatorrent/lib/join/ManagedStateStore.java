package com.datatorrent.lib.join;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.managed.ManagedTimeStateImpl;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.netlet.util.Slice;

public class ManagedStateStore extends ManagedTimeStateImpl implements JoinStore
{
  private static int i = 0;
  private final transient KryoSerializableStreamCodec kryo = new KryoSerializableStreamCodec();
  private transient Class outputClass;
  private String outputClassStr = "com.datatorrent.lib.join.TimeEventImpl";
  @Override
  public List<?> getValidTuples(Object tuple)
  {
    TimeEvent te = (TimeEvent)tuple;
    Object key = te.getEventKey();

    //LOG.info("getValid Tuple - 1: {} -> {}", key);
    Slice value = super.getSync(0, kryo.toByteArray(key));
    List<Object> output = new ArrayList<>();
    if (value != null) {
      //LOG.info("getValid Tuple - 1: {} -> {}", key,value.buffer);
      output.add(kryo.fromByteArray(value));
      //LOG.info("getValid Tuple - 2: {}", key);
    }
    //LOG.info("getValid Tuple - 3: {}", key);
    return output;
  }

  @Override
  public boolean put(Object tuple)
  {
    TimeEvent te = (TimeEvent)tuple;
    Object key = te.getEventKey();
    Object value = te.getValue();
    super.put(0,te.getTime(),kryo.toByteArray(key), kryo.toByteArray(tuple));
    return true;
  }

  @Override
  public List<?> getUnMatchedTuples()
  {
    return null;
  }

  @Override
  public void isOuterJoin(boolean isOuter)
  {
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    try {
      outputClass = this.getClass().getClassLoader().loadClass(outputClassStr);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    ((FileAccessFSImpl)getFileAccess()).setBasePath(context.getValue(DAG.APPLICATION_PATH) + "/" + "bucket_data_" + i);
    i++;
    super.setup(context);
  }

  public Class getOutputClass()
  {
    return outputClass;
  }

  public void setOutputClass(Class outputClass)
  {
    this.outputClass = outputClass;
  }

  private static final transient Logger LOG = LoggerFactory.getLogger(ManagedStateStore.class);
}


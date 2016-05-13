package com.datatorrent.demos.dimensions;
import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.managed.ManagedStateImpl;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.netlet.util.Slice;

public class ManagedStateIntOperator implements Operator, Operator.CheckpointListener, Operator.CheckpointNotificationListener
{
  private static final Logger LOG = LoggerFactory.getLogger(ManagedStateIntOperator.class);
  private ManagedStateImpl managedState;
  private long time = System.currentTimeMillis();
  private transient Decomposer dc = new Decomposer.DefaultDecomposer();
  boolean isSearch = false;
  public ManagedStateIntOperator()
  {
    managedState = new ManagedStateImpl();
  }

  @OutputPortFieldAnnotation
  public transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();

  @InputPortFieldAnnotation
  public transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
  {
    @Override
    public void process(Integer tuple)
    {
      processTuple(tuple);
    }
  };

  void processTuple(Integer tuple)
  {
    if (isSearch) {
      Object value = get(tuple);
      if (value != null) {
        output.emit(value);
      }
    } else {
      put(tuple);
    }
  }

  public Object get(Integer key)
  {
    byte[] keybytes = dc.decompose(key);
    LOG.info("Get: {}", key);
    Slice value = managedState.getSync(0, new Slice(keybytes));
    if (value != null && value.length != 0) {
      LOG.info("Get - Value: {}", value);
      return dc.compose(value.buffer);
    }
    return null;
  }

  public boolean put(Object tuple)
  {
    LOG.info("Put: {}", tuple);
    byte[] keybytes = dc.decompose(tuple);
    byte[] valuebytes = dc.decompose(tuple);
    managedState.put(0, new Slice(keybytes), new Slice(keybytes));
    return true;
  }

  /*public Object get(Integer key)
  {
    byte[] keybytes = BigInteger.valueOf(key).toByteArray();
    Slice value = managedState.getSync(0, new Slice(keybytes));
    if (value != null && value.length != 0) {
      return new BigInteger(value.buffer).intValue();
    }
    return null;
  }

  public boolean put(Integer tuple)
  {
    managedState.put(0, new Slice(BigInteger.valueOf(tuple).toByteArray()),
      new Slice(BigInteger.valueOf(tuple).toByteArray()));
    return true;
  }*/

  @Override
  public void beginWindow(long windowId)
  {
    managedState.beginWindow(windowId);
    time = System.currentTimeMillis();
  }

  @Override
  public void endWindow()
  {
    managedState.endWindow();
    isSearch = !isSearch;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    ((FileAccessFSImpl)managedState.getFileAccess()).setBasePath(context.getValue(DAG.APPLICATION_PATH) + "/" + "bucket_data");
    managedState.setup(context);
  }

  @Override
  public void teardown()
  {
    managedState.teardown();
  }

  @Override
  public void checkpointed(long windowId)
  {
    managedState.checkpointed(windowId);
  }

  @Override
  public void committed(long windowId)
  {
    managedState.committed(windowId);
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    managedState.beforeCheckpoint(windowId);
  }
}

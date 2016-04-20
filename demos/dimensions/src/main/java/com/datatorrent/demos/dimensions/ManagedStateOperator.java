package com.datatorrent.demos.dimensions;

import org.apache.apex.malhar.lib.state.managed.ManagedStateImpl;
import org.apache.commons.lang.ClassUtils;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.netlet.util.Slice;

public class ManagedStateOperator implements Operator, Operator.CheckpointListener, Operator.CheckpointNotificationListener
{
  protected Class inputClass;
  private String inputClassStr;
  private ManagedStateImpl managedState;
  private transient PojoUtils.Getter keyGetter;
  private String keyField;
  public ManagedStateOperator()
  {
    managedState = new ManagedStateImpl();
  }

  @OutputPortFieldAnnotation
  public transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();

  @InputPortFieldAnnotation
  public transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object tuple)
    {
      processTuple(tuple);
    }
  };

  void processTuple(Object tuple)
  {
    Slice keyS = new Slice(keyGetter.get(tuple).toString().getBytes());
    Slice value = new Slice(tuple.toString().getBytes());
    managedState.put(0, keyS, value);
    output.emit(value);
  }

  @Override
  public void beginWindow(long windowId)
  {
    managedState.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    managedState.endWindow();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    ((FileAccessFSImpl)managedState.getFileAccess()).setBasePath(context.getValue(DAG.APPLICATION_PATH) + "/" + "bucket_data");
    managedState.setup(context);
    try {
      this.inputClass = this.getClass().getClassLoader().loadClass(inputClassStr);
      Class c = ClassUtils.primitiveToWrapper(inputClass.getField(keyField).getType());
      keyGetter = PojoUtils.createGetter(inputClass, keyField, c);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
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

  public String getInputClassStr()
  {
    return inputClassStr;
  }

  public void setInputClassStr(String inputClassStr)
  {
    this.inputClassStr = inputClassStr;
  }

  public String getKeyField()
  {
    return keyField;
  }

  public void setKeyField(String keyField)
  {
    this.keyField = keyField;
  }
}

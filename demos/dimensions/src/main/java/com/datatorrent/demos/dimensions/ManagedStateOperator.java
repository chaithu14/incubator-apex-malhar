package com.datatorrent.demos.dimensions;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.apex.malhar.lib.state.managed.ManagedStateImpl;
import org.apache.commons.lang.ClassUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.lib.join.TimeEvent;
import com.datatorrent.lib.join.TimeEventImpl;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.netlet.util.Slice;

public class ManagedStateOperator implements Operator, Operator.CheckpointListener, Operator.CheckpointNotificationListener
{
  protected Class inputClass;
  private String inputClassStr;
  private ManagedStateImpl managedState;
  private transient PojoUtils.Getter keyGetter;
  private String keyField;
  private long time = System.currentTimeMillis();
  private transient Decomposer dc = new Decomposer.DefaultDecomposer();
  boolean isSearch = false;
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
    if (isSearch) {
      Object value = get(keyGetter.get(tuple));
      output.emit(value);
    } else {
      put(tuple);
    }
  }

  public Object get(Object key)
  {
    byte[] keybytes = dc.decompose(key);
    Slice value = managedState.getSync(0, new Slice(keybytes));
    List<Object> output = new ArrayList<>();
    if (value != null) {
      output.add(dc.compose(value.buffer));
    }
    return output;
  }

  public boolean put(Object tuple)
  {
    byte[] keybytes = dc.decompose(keyGetter.get(tuple));
    byte[] valuebytes = dc.decompose(tuple);
    managedState.put(0, new Slice(keybytes), new Slice(keybytes));
    return true;
  }

  /*public Object get(Object key)
  {
    ByteArrayOutputStream bos1 = new ByteArrayOutputStream();
    Output output1 = new Output(bos1);
    kryo.writeObject(output1, key);
    output1.close();
    Slice value = managedState.getSync(0, new Slice(bos1.toByteArray()));
    List<Object> output = new ArrayList<>();
    if (value != null) {
      Input lInput = new Input(value.buffer);
      //output.add(kryo.readObject(lInput, outputClass));
      output.add(kryo.readClassAndObject(lInput));
    }
    return output;
  }

  public boolean put(Object tuple)
  {
    TimeEventImpl te = new TimeEventImpl(keyGetter.get(tuple), time, tuple);
    Object key = te.getEventKey();
    Object value = te.getValue();
    ByteArrayOutputStream bos1 = new ByteArrayOutputStream();
    Output output1 = new Output(bos1);
    kryo.writeObject(output1, key);
    output1.close();
    ByteArrayOutputStream bos2 = new ByteArrayOutputStream();
    Output output2 = new Output(bos2);
    kryo.writeClassAndObject(output2, (SalesEvent)value);
    output2.close();
    managedState.put(0, new Slice(bos1.toByteArray()), new Slice(bos2.toByteArray()));
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

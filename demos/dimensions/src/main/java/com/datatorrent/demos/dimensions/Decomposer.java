package com.datatorrent.demos.dimensions;

import java.io.ByteArrayOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public interface Decomposer<T>
{

  /**
   * Decompose the object into the byte-array
   *
   * @param object
   *          the object to be decomposed
   */
  byte[] decompose(T object);

  T compose(byte[] by);

  public class DefaultDecomposer implements Decomposer<Object>
  {
    private transient Kryo kryo = new Kryo();
    /**
     * Decompose the object
     */
    @Override
    public byte[] decompose(Object object)
    {
      if (object == null) {
        return null;
      }
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Output output = new Output(bos);
      kryo.writeClassAndObject(output, object);
      output.flush();
      return bos.toByteArray();
    }

    @Override
    public Object compose(byte[] by)
    {
      Input lInput = new Input(by);
      return kryo.readClassAndObject(lInput);
    }
  }
}
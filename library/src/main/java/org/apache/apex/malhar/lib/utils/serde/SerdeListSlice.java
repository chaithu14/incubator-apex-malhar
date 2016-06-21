package org.apache.apex.malhar.lib.utils.serde;

import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

/**
 * Created by tfarkas on 6/11/16.
 */
public class SerdeListSlice<T> implements Serde<List<T>, Slice>
{
  @NotNull
  private Serde<T, Slice> serde;

  public SerdeListSlice(@NotNull Serde<T, Slice> serde)
  {
    this.serde = Preconditions.checkNotNull(serde);
  }

  @Override
  public Slice serialize(List<T> objects)
  {
    Slice[] slices = new Slice[objects.size()];

    int size = 4;

    for (int index = 0; index < objects.size(); index++) {
      Slice slice = serde.serialize(objects.get(index));
      slices[index] = slice;
      size += slice.length;
    }

    byte[] bytes = new byte[size];
    int offset = 0;

    byte[] sizeBytes = GPOUtils.serializeInt(objects.size());
    System.arraycopy(sizeBytes, 0, bytes, offset, 4);
    offset += 4;

    for (int index = 0; index < slices.length; index++) {
      Slice slice = slices[index];
      System.arraycopy(slice.buffer, slice.offset, bytes, offset, slice.length);
      offset += slice.length;
    }

    return new Slice(bytes);
  }

  @Override
  public List<T> deserialize(Slice slice, MutableInt offset)
  {
    MutableInt sliceOffset = new MutableInt(slice.offset + offset.intValue());

    int numElements = GPOUtils.deserializeInt(slice.buffer, sliceOffset);
    List<T> list = Lists.newArrayListWithCapacity(numElements);
    sliceOffset.subtract(slice.offset);

    for (int index = 0; index < numElements; index++) {
      T object = serde.deserialize(slice, sliceOffset);
      list.add(object);
    }

    offset.setValue(sliceOffset.intValue());
    return list;
  }

  @Override
  public List<T> deserialize(Slice slice)
  {
    return deserialize(slice, new MutableInt(0));
  }
}

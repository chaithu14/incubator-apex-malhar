package org.apache.apex.malhar.lib.utils.serde;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

/**
 * Created by tfarkas on 6/14/16.
 */
public class SerdeStringSlice implements Serde<String, Slice>
{
  @Override
  public Slice serialize(String object)
  {
    return new Slice(GPOUtils.serializeString(object));
  }

  @Override
  public String deserialize(Slice object, MutableInt offset)
  {
    offset.add(object.offset);
    String string = GPOUtils.deserializeString(object.buffer, offset);
    offset.subtract(object.offset);
    return string;
  }

  @Override
  public String deserialize(Slice object)
  {
    return deserialize(object, new MutableInt(0));
  }
}

package org.apache.apex.malhar.lib.utils.serde;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.datatorrent.netlet.util.Slice;

/**
 * Created by tfarkas on 6/19/16.
 */
public class SerdeListSliceTest
{
  @Test
  public void simpleSerdeTest()
  {
    SerdeListSlice<String> serdeList = new SerdeListSlice<String>(new SerdeStringSlice());

    List<String> stringList = Lists.newArrayList("a", "b", "c");

    Slice slice = serdeList.serialize(stringList);

    List<String> deserializedList = serdeList.deserialize(slice);

    Assert.assertEquals(stringList, deserializedList);
  }
}

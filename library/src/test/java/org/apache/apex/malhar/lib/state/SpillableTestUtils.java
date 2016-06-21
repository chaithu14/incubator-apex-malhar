package org.apache.apex.malhar.lib.state;

import java.util.List;

import org.junit.Assert;

import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SerdeListSlice;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;
import org.apache.apex.malhar.lib.utils.serde.SliceUtils;
import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

/**
 * Created by tfarkas on 6/18/16.
 */
public class SpillableTestUtils
{
  public static SerdeStringSlice SERDE_STRING_SLICE = new SerdeStringSlice();
  public static SerdeListSlice<String> SERDE_STRING_LIST_SLICE = new SerdeListSlice(new SerdeStringSlice());


  private SpillableTestUtils()
  {
    //Shouldn't instantiate this
  }

  public static Slice getKeySlice(byte[] id, String key)
  {
    return SliceUtils.concatenate(id, SERDE_STRING_SLICE.serialize(key));
  }

  public static Slice getKeySlice(byte[] id, int index, String key)
  {
    return SliceUtils.concatenate(id,
        SliceUtils.concatenate(GPOUtils.serializeInt(index),
            SERDE_STRING_SLICE.serialize(key)));
  }

  public static void checkValue(SpillableStateStore store, long bucketId, String key,
      byte[] prefix, String expectedValue)
  {
    checkValue(store, bucketId, SliceUtils.concatenate(prefix, SERDE_STRING_SLICE.serialize(key)).buffer,
        expectedValue, prefix.length, SERDE_STRING_SLICE);
  }

  public static void checkValue(SpillableStateStore store, long bucketId,
      byte[] prefix, int index, List<String> expectedValue)
  {
    checkValue(store, bucketId, SliceUtils.concatenate(prefix, GPOUtils.serializeInt(index)), expectedValue, prefix
        .length, SERDE_STRING_LIST_SLICE);
  }

  public static <T> void  checkValue(SpillableStateStore store, long bucketId, byte[] bytes,
      T expectedValue, int offset, Serde<T, Slice> serde)
  {
    Slice slice = store.getSync(bucketId, new Slice(bytes));

    if (slice == null || slice.length == 0) {
      if (expectedValue != null) {
        Assert.assertEquals(expectedValue, slice);
      } else {
        return;
      }
    }

    T string = serde.deserialize(slice, new MutableInt(offset));

    Assert.assertEquals(expectedValue, string);
  }
}

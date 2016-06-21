package org.apache.apex.malhar.lib.state;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.state.spillable.WindowBoundedMapCache;

import com.google.common.collect.Sets;

/**
 * Created by tfarkas on 6/4/16.
 */
public class WindowBoundedMapCacheTest
{
  @Test
  public void simplePutGetTest()
  {
    WindowBoundedMapCache<String, String> cache = new WindowBoundedMapCache<>();

    long windowId = 0L;

    windowId++;

    cache.put("1", "a");
    Assert.assertEquals("a", cache.get("1"));

    cache.endWindow();

    windowId++;

    Assert.assertEquals("a", cache.get("1"));

    cache.endWindow();
  }

  @Test
  public void getChangedGetRemovedTest()
  {
    WindowBoundedMapCache<String, String> cache = new WindowBoundedMapCache<>();

    long windowId = 0L;

    windowId++;

    cache.put("1", "a");
    cache.put("2", "b");

    Assert.assertEquals(Sets.newHashSet("1", "2"), cache.getChangedKeys());
    Assert.assertEquals(Sets.newHashSet(), cache.getRemovedKeys());

    cache.endWindow();

    windowId++;

    cache.remove("1");

    Assert.assertEquals(Sets.newHashSet(), cache.getChangedKeys());
    Assert.assertEquals(Sets.newHashSet("1"), cache.getRemovedKeys());

    Assert.assertEquals(null, cache.get("1"));
    Assert.assertEquals("b", cache.get("2"));

    cache.endWindow();

    windowId++;

    Assert.assertEquals(Sets.newHashSet(), cache.getChangedKeys());
    Assert.assertEquals(Sets.newHashSet(), cache.getRemovedKeys());

    cache.endWindow();
  }

  @Test
  public void expirationTest() throws Exception
  {
    WindowBoundedMapCache<String, String> cache = new WindowBoundedMapCache<>(2);

    long windowId = 0L;

    windowId++;

    cache.put("1", "a");
    Thread.sleep(1L);
    cache.put("2", "b");
    Thread.sleep(1L);
    cache.put("3", "c");

    Assert.assertEquals(Sets.newHashSet("1", "2", "3"), cache.getChangedKeys());

    cache.endWindow();

    windowId++;

    Assert.assertEquals(null, cache.get("1"));
    Assert.assertEquals("b", cache.get("2"));
    Assert.assertEquals("c", cache.get("3"));

    Assert.assertEquals(Sets.newHashSet(), cache.getChangedKeys());
    Assert.assertEquals(Sets.newHashSet(), cache.getRemovedKeys());

    cache.endWindow();
  }
}

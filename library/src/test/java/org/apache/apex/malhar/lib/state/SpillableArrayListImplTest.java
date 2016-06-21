package org.apache.apex.malhar.lib.state;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.state.spillable.SpillableArrayListImpl;
import org.apache.apex.malhar.lib.state.spillable.inmem.InMemSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;

import com.google.common.collect.Lists;

/**
 * Created by tfarkas on 6/19/16.
 */
public class SpillableArrayListImplTest
{
  public static final byte[] ID1 = new byte[]{(byte)0};
  public static final byte[] ID2 = new byte[]{(byte)1};

  @Test
  public void simpleAddGetAndSetTest1()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    SpillableArrayListImpl<String> list = new SpillableArrayListImpl<>(0L, ID1, store,
        new SerdeStringSlice(), 1);

    long windowId = 0L;
    list.beginWindow(windowId);
    windowId++;

    checkOutOfBounds(list, 0);
    Assert.assertEquals(0, list.size());

    list.add("a");

    checkOutOfBounds(list, 1);
    Assert.assertEquals(1, list.size());

    Assert.assertEquals("a", list.get(0));

    list.addAll(Lists.newArrayList("a", "b", "c"));

    Assert.assertEquals(4, list.size());

    Assert.assertEquals("a", list.get(0));
    Assert.assertEquals("a", list.get(1));
    Assert.assertEquals("b", list.get(2));
    Assert.assertEquals("c", list.get(3));

    checkOutOfBounds(list, 4);

    list.endWindow();

    SpillableTestUtils.checkValue(store, 0L, ID1, 0, Lists.<String>newArrayList("a"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 1, Lists.<String>newArrayList("a"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 2, Lists.<String>newArrayList("b"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 3, Lists.<String>newArrayList("c"));

    list.beginWindow(windowId);
    windowId++;

    Assert.assertEquals(4, list.size());

    Assert.assertEquals("a", list.get(0));
    Assert.assertEquals("a", list.get(1));
    Assert.assertEquals("b", list.get(2));
    Assert.assertEquals("c", list.get(3));

    list.add("tt");
    list.add("ab");
    list.add("99");
    list.add("oo");

    Assert.assertEquals("tt", list.get(4));
    Assert.assertEquals("ab", list.get(5));
    Assert.assertEquals("99", list.get(6));
    Assert.assertEquals("oo", list.get(7));

    list.set(1, "111");

    Assert.assertEquals("a", list.get(0));
    Assert.assertEquals("111", list.get(1));
    Assert.assertEquals("b", list.get(2));
    Assert.assertEquals("c", list.get(3));
    Assert.assertEquals("tt", list.get(4));
    Assert.assertEquals("ab", list.get(5));
    Assert.assertEquals("99", list.get(6));
    Assert.assertEquals("oo", list.get(7));

    list.endWindow();

    SpillableTestUtils.checkValue(store, 0L, ID1, 0, Lists.<String>newArrayList("a"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 1, Lists.<String>newArrayList("111"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 2, Lists.<String>newArrayList("b"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 3, Lists.<String>newArrayList("c"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 4, Lists.<String>newArrayList("tt"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 5, Lists.<String>newArrayList("ab"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 6, Lists.<String>newArrayList("99"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 7, Lists.<String>newArrayList("oo"));
  }

  @Test
  public void simpleAddGetAndSetTest3()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    SpillableArrayListImpl<String> list = new SpillableArrayListImpl<>(0L, ID1, store,
        new SerdeStringSlice(), 3);

    long windowId = 0L;
    list.beginWindow(windowId);
    windowId++;

    checkOutOfBounds(list, 0);
    Assert.assertEquals(0, list.size());

    list.add("a");

    checkOutOfBounds(list, 1);
    Assert.assertEquals(1, list.size());

    Assert.assertEquals("a", list.get(0));

    list.addAll(Lists.newArrayList("a", "b", "c", "d", "e", "f", "g"));

    Assert.assertEquals(8, list.size());

    Assert.assertEquals("a", list.get(0));
    Assert.assertEquals("a", list.get(1));
    Assert.assertEquals("b", list.get(2));
    Assert.assertEquals("c", list.get(3));
    Assert.assertEquals("d", list.get(4));
    Assert.assertEquals("e", list.get(5));
    Assert.assertEquals("f", list.get(6));
    Assert.assertEquals("g", list.get(7));

    checkOutOfBounds(list, 20);

    list.endWindow();

    SpillableTestUtils.checkValue(store, 0L, ID1, 0, Lists.<String>newArrayList("a", "a", "b"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 1, Lists.<String>newArrayList("c", "d", "e"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 2, Lists.<String>newArrayList("f", "g"));

    list.beginWindow(windowId);
    windowId++;

    Assert.assertEquals(8, list.size());

    Assert.assertEquals("a", list.get(0));
    Assert.assertEquals("a", list.get(1));
    Assert.assertEquals("b", list.get(2));
    Assert.assertEquals("c", list.get(3));
    Assert.assertEquals("d", list.get(4));
    Assert.assertEquals("e", list.get(5));
    Assert.assertEquals("f", list.get(6));
    Assert.assertEquals("g", list.get(7));

    list.add("tt");
    list.add("ab");
    list.add("99");
    list.add("oo");

    Assert.assertEquals("tt", list.get(8));
    Assert.assertEquals("ab", list.get(9));
    Assert.assertEquals("99", list.get(10));
    Assert.assertEquals("oo", list.get(11));

    list.endWindow();

    SpillableTestUtils.checkValue(store, 0L, ID1, 0, Lists.<String>newArrayList("a", "a", "b"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 1, Lists.<String>newArrayList("c", "d", "e"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 2, Lists.<String>newArrayList("f", "g", "tt"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 3, Lists.<String>newArrayList("ab", "99", "oo"));

    list.beginWindow(windowId);
    windowId++;

    list.set(1, "111");
    list.set(3, "222");
    list.set(5, "333");
    list.set(11, "444");

    Assert.assertEquals("a", list.get(0));
    Assert.assertEquals("111", list.get(1));
    Assert.assertEquals("b", list.get(2));
    Assert.assertEquals("222", list.get(3));
    Assert.assertEquals("d", list.get(4));
    Assert.assertEquals("333", list.get(5));
    Assert.assertEquals("f", list.get(6));
    Assert.assertEquals("g", list.get(7));
    Assert.assertEquals("tt", list.get(8));
    Assert.assertEquals("ab", list.get(9));
    Assert.assertEquals("99", list.get(10));
    Assert.assertEquals("444", list.get(11));

    list.endWindow();

    SpillableTestUtils.checkValue(store, 0L, ID1, 0, Lists.<String>newArrayList("a", "111", "b"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 1, Lists.<String>newArrayList("222", "d", "333"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 2, Lists.<String>newArrayList("f", "g", "tt"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 3, Lists.<String>newArrayList("ab", "99", "444"));
  }

  @Test
  public void simpleMultiListTest()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    SpillableArrayListImpl<String> list1 = new SpillableArrayListImpl<>(0L, ID1, store,
        new SerdeStringSlice(), 1);

    SpillableArrayListImpl<String> list2 = new SpillableArrayListImpl<>(0L, ID2, store,
        new SerdeStringSlice(), 1);

    long windowId = 0L;
    list1.beginWindow(windowId);
    list2.beginWindow(windowId);
    windowId++;

    checkOutOfBounds(list1, 0);
    Assert.assertEquals(0, list1.size());

    list1.add("a");

    checkOutOfBounds(list2, 0);

    list2.add("2a");

    checkOutOfBounds(list1, 1);
    checkOutOfBounds(list2, 1);

    Assert.assertEquals(1, list1.size());
    Assert.assertEquals(1, list2.size());

    Assert.assertEquals("a", list1.get(0));
    Assert.assertEquals("2a", list2.get(0));

    list1.addAll(Lists.newArrayList("a", "b", "c"));
    list2.addAll(Lists.newArrayList("2a", "2b"));

    Assert.assertEquals(4, list1.size());
    Assert.assertEquals(3, list2.size());

    Assert.assertEquals("a", list1.get(0));
    Assert.assertEquals("a", list1.get(1));
    Assert.assertEquals("b", list1.get(2));
    Assert.assertEquals("c", list1.get(3));

    Assert.assertEquals("2a", list2.get(0));
    Assert.assertEquals("2a", list2.get(1));
    Assert.assertEquals("2b", list2.get(2));

    checkOutOfBounds(list1, 4);
    checkOutOfBounds(list2, 3);

    list1.endWindow();
    list2.endWindow();

    SpillableTestUtils.checkValue(store, 0L, ID1, 0, Lists.<String>newArrayList("a"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 1, Lists.<String>newArrayList("a"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 2, Lists.<String>newArrayList("b"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 3, Lists.<String>newArrayList("c"));

    SpillableTestUtils.checkValue(store, 0L, ID2, 0, Lists.<String>newArrayList("2a"));
    SpillableTestUtils.checkValue(store, 0L, ID2, 1, Lists.<String>newArrayList("2a"));
    SpillableTestUtils.checkValue(store, 0L, ID2, 2, Lists.<String>newArrayList("2b"));

    list1.beginWindow(windowId);
    list2.beginWindow(windowId);
    windowId++;

    Assert.assertEquals(4, list1.size());
    Assert.assertEquals(3, list2.size());

    Assert.assertEquals("a", list1.get(0));
    Assert.assertEquals("a", list1.get(1));
    Assert.assertEquals("b", list1.get(2));
    Assert.assertEquals("c", list1.get(3));

    Assert.assertEquals("2a", list2.get(0));
    Assert.assertEquals("2a", list2.get(1));
    Assert.assertEquals("2b", list2.get(2));

    list1.add("tt");
    list1.add("ab");
    list1.add("99");
    list1.add("oo");

    list2.add("2tt");
    list2.add("2ab");

    Assert.assertEquals("tt", list1.get(4));
    Assert.assertEquals("ab", list1.get(5));
    Assert.assertEquals("99", list1.get(6));
    Assert.assertEquals("oo", list1.get(7));

    Assert.assertEquals("2tt", list2.get(3));
    Assert.assertEquals("2ab", list2.get(4));

    list1.set(1, "111");
    list2.set(1, "2111");

    Assert.assertEquals("a", list1.get(0));
    Assert.assertEquals("111", list1.get(1));
    Assert.assertEquals("b", list1.get(2));
    Assert.assertEquals("c", list1.get(3));
    Assert.assertEquals("tt", list1.get(4));
    Assert.assertEquals("ab", list1.get(5));
    Assert.assertEquals("99", list1.get(6));
    Assert.assertEquals("oo", list1.get(7));

    Assert.assertEquals("2a", list2.get(0));
    Assert.assertEquals("2111", list2.get(1));
    Assert.assertEquals("2b", list2.get(2));
    Assert.assertEquals("2tt", list2.get(3));
    Assert.assertEquals("2ab", list2.get(4));

    list1.endWindow();
    list2.endWindow();

    SpillableTestUtils.checkValue(store, 0L, ID1, 0, Lists.<String>newArrayList("a"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 1, Lists.<String>newArrayList("111"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 2, Lists.<String>newArrayList("b"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 3, Lists.<String>newArrayList("c"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 4, Lists.<String>newArrayList("tt"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 5, Lists.<String>newArrayList("ab"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 6, Lists.<String>newArrayList("99"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 7, Lists.<String>newArrayList("oo"));

    SpillableTestUtils.checkValue(store, 0L, ID2, 0, Lists.<String>newArrayList("2a"));
    SpillableTestUtils.checkValue(store, 0L, ID2, 1, Lists.<String>newArrayList("2111"));
    SpillableTestUtils.checkValue(store, 0L, ID2, 2, Lists.<String>newArrayList("2b"));
    SpillableTestUtils.checkValue(store, 0L, ID2, 3, Lists.<String>newArrayList("2tt"));
    SpillableTestUtils.checkValue(store, 0L, ID2, 4, Lists.<String>newArrayList("2ab"));
  }

  private void checkOutOfBounds(SpillableArrayListImpl<String> list, int index)
  {
    boolean exceptionThrown = false;

    try {
      list.get(index);
    } catch (IndexOutOfBoundsException ex) {
      exceptionThrown = true;
    }

    Assert.assertTrue(exceptionThrown);
  }
}

package org.apache.apex.malhar.lib.state.spillable;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.BucketedState;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SliceUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

public class SpillableByteMapImpl<K, V> implements Spillable.SpillableByteMap<K, V>, Spillable.SpillableComponent
{
  @NotNull
  private SpillableStateStore store;
  @NotNull
  private byte[] identifier;
  private long bucket;
  @NotNull
  private Serde<K, Slice> serdeKey;
  @NotNull
  private Serde<V, Slice> serdeValue;

  private int size = 0;

  private transient WindowBoundedMapCache<K, V> cache = new WindowBoundedMapCache<>();
  private transient MutableInt tempOffset = new MutableInt();

  private SpillableByteMapImpl()
  {
    //for kryo
  }

  public SpillableByteMapImpl(SpillableStateStore store, byte[] identifier, long bucket, Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue)
  {
    this.store = Preconditions.checkNotNull(store);
    this.identifier = Preconditions.checkNotNull(identifier);
    this.bucket = bucket;
    this.serdeKey = Preconditions.checkNotNull(serdeKey);
    this.serdeValue = Preconditions.checkNotNull(serdeValue);
  }

  @Override
  public int size()
  {
    return size;
  }

  @Override
  public boolean isEmpty()
  {
    return size == 0;
  }

  @Override
  public boolean containsKey(Object o)
  {
    return get(o) != null;
  }

  @Override
  public boolean containsValue(Object o)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public V get(Object o)
  {
    K key = (K)o;

    if (cache.getRemovedKeys().contains(key)) {
      return null;
    }

    V val = cache.get(key);

    if (val != null) {
      return val;
    }

    Slice valSlice = store.getSync(bucket, SliceUtils.concatenate(identifier, serdeKey.serialize(key)));

    if (valSlice == null || valSlice == BucketedState.EXPIRED || valSlice.length == 0) {
      return null;
    }

    tempOffset.setValue(valSlice.offset + identifier.length);
    return serdeValue.deserialize(valSlice, tempOffset);
  }

  @Override
  public V put(K k, V v)
  {
    V value = get(k);

    if (value == null) {
      size++;
    }

    cache.put(k, v);

    return value;
  }

  @Override
  public V remove(Object o)
  {
    V value = get(o);

    if (value != null) {
      size--;
    }

    cache.remove((K)o);

    return value;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map)
  {
    for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void clear()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<K> keySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<V> values()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Entry<K, V>> entrySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    for (K key: cache.getChangedKeys()) {
      store.put(this.bucket, SliceUtils.concatenate(identifier, serdeKey.serialize(key)),
          SliceUtils.concatenate(identifier, serdeValue.serialize(cache.get(key))));
    }

    for (K key: cache.getRemovedKeys()) {
      store.put(this.bucket, SliceUtils.concatenate(identifier, serdeKey.serialize(key)),
          new Slice(ArrayUtils.EMPTY_BYTE_ARRAY));
    }

    cache.endWindow();
  }

  @Override
  public void teardown()
  {
  }
}

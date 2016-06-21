package org.apache.apex.malhar.lib.state.spillable.inmem;

import java.util.Map;
import java.util.concurrent.Future;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * Created by tfarkas on 6/6/16.
 */
public class InMemSpillableStateStore implements SpillableStateStore
{
  private Map<Long, Map<Slice, Slice>> store = Maps.newHashMap();

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

  }

  @Override
  public void teardown()
  {

  }

  @Override
  public void put(long bucketId, @NotNull Slice key, @NotNull Slice value)
  {
    Map<Slice, Slice> bucket = store.get(bucketId);

    if (bucket == null) {
      bucket = Maps.newHashMap();
      store.put(bucketId, bucket);
    }

    bucket.put(key, value);
  }

  @Override
  public Slice getSync(long bucketId, @NotNull Slice key)
  {
    Map<Slice, Slice> bucket = store.get(bucketId);

    if (bucket == null) {
      bucket = Maps.newHashMap();
      store.put(bucketId, bucket);
    }

    return bucket.get(key);
  }

  @Override
  public Future<Slice> getAsync(long bucketId, @NotNull Slice key)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void beforeCheckpoint(long l)
  {
  }

  @Override
  public void checkpointed(long l)
  {
  }

  @Override
  public void committed(long l)
  {
  }

  @Override
  public String toString()
  {
    return store.toString();
  }
}

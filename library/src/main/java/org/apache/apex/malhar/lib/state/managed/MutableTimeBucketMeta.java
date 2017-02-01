package org.apache.apex.malhar.lib.state.managed;

import javax.validation.constraints.NotNull;

import com.datatorrent.netlet.util.Slice;

/**
 * Represents time bucket meta information which can be changed.
 * The updates to an instance and read/creation of {@link #immutableTimeBucketMeta} belonging to it are synchronized
 * as different threads are updating and reading from it.<br/>
 *
 * The instance is updated when data from window files are transferred to bucket files and
 * {@link Bucket.DefaultBucket} reads the immutable time bucket meta.
 */
public class MutableTimeBucketMeta extends TimeBucketMeta
{
  private transient TimeBucketMeta immutableTimeBucketMeta;

  private volatile boolean changed;

  public MutableTimeBucketMeta(long bucketId, long timeBucketId)
  {
    super(bucketId, timeBucketId);
  }

  synchronized void updateTimeBucketMeta(long lastTransferredWindow, long bytes, @NotNull Slice firstKey)
  {
    changed = true;
    super.setLastTransferredWindowId(lastTransferredWindow);
    super.setSizeInBytes(bytes);
    super.setFirstKey(firstKey);
  }

  synchronized TimeBucketMeta getImmutableTimeBucketMeta()
  {
    if (immutableTimeBucketMeta == null || changed) {

      immutableTimeBucketMeta = new TimeBucketMeta(getBucketId(), getTimeBucketId());
      immutableTimeBucketMeta.setLastTransferredWindowId(getLastTransferredWindowId());
      immutableTimeBucketMeta.setSizeInBytes(getSizeInBytes());
      immutableTimeBucketMeta.setFirstKey(getFirstKey());
      changed = false;
    }
    return immutableTimeBucketMeta;
  }

}



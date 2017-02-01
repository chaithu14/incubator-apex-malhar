package org.apache.apex.malhar.lib.state.managed;

import java.util.Objects;

import javax.validation.constraints.NotNull;

import com.datatorrent.netlet.util.Slice;

/**
 * This serves the readers - {@link Bucket.DefaultBucket}.
 * It is immutable and accessible outside the package unlike {@link BucketsFileSystem.MutableTimeBucketMeta}.
 */
public class TimeBucketMeta implements Comparable<TimeBucketMeta>
{
  private final long bucketId;
  private final long timeBucketId;
  private long lastTransferredWindowId = -1;
  private long sizeInBytes;
  private Slice firstKey;

  private TimeBucketMeta()
  {
    //for kryo
    bucketId = -1;
    timeBucketId = -1;
  }

  public TimeBucketMeta(long bucketId, long timeBucketId)
  {
    this.bucketId = bucketId;
    this.timeBucketId = timeBucketId;
  }

  public long getLastTransferredWindowId()
  {
    return lastTransferredWindowId;
  }

  public long getSizeInBytes()
  {
    return this.sizeInBytes;
  }

  public long getBucketId()
  {
    return bucketId;
  }

  public long getTimeBucketId()
  {
    return timeBucketId;
  }

  public Slice getFirstKey()
  {
    return firstKey;
  }

  public void setLastTransferredWindowId(long lastTransferredWindowId)
  {
    this.lastTransferredWindowId = lastTransferredWindowId;
  }

  public void setSizeInBytes(long sizeInBytes)
  {
    this.sizeInBytes = sizeInBytes;
  }

  public void setFirstKey(Slice firstKey)
  {
    this.firstKey = firstKey;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimeBucketMeta)) {
      return false;
    }

    TimeBucketMeta that = (TimeBucketMeta)o;

    return bucketId == that.bucketId && timeBucketId == that.timeBucketId;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(bucketId, timeBucketId);
  }

  @Override
  public int compareTo(@NotNull TimeBucketMeta o)
  {
    if (bucketId < o.bucketId) {
      return -1;
    }
    if (bucketId > o.bucketId) {
      return 1;
    }
    if (timeBucketId < o.timeBucketId) {
      return -1;
    }
    if (timeBucketId > o.timeBucketId) {
      return 1;
    }
    return 0;
  }
}

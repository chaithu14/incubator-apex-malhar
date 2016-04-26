package com.datatorrent.lib.io.fs;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;
import com.datatorrent.api.Context;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.BlockReader;
import com.datatorrent.lib.io.block.ReaderContext;

/**
 * S3BlockReader extends from BlockReader and serves the functionality of reads and
 * parse Block metadata
 */
public class S3BlockReader extends BlockReader
{
  private transient String s3bucketUri;

  public S3BlockReader()
  {
    this.readerContext = new S3BlockReaderContext();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    s3bucketUri = fs.getScheme() + "://" + extractBucket(uri);
  }

  @VisibleForTesting
  protected String extractBucket(String s3uri)
  {
    return s3uri.substring(s3uri.indexOf('@') + 1, s3uri.indexOf("/", s3uri.indexOf('@')));
  }

  @Override
  protected FSDataInputStream setupStream(BlockMetadata.FileBlockMetadata block) throws IOException
  {
    FSDataInputStream ins = fs.open(new Path(s3bucketUri + block.getFilePath()));
    ins.seek(block.getOffset());
    return ins;
  }

  /**
   * BlockReadeContext for reading S3 Blocks.<br/>
   * This should use read API without offset.
   */
  private static class S3BlockReaderContext extends ReaderContext.FixedBytesReaderContext<FSDataInputStream>
  {
    @Override
    protected Entity readEntity() throws IOException
    {
      entity.clear();
      int bytesToRead = length;
      if (offset + length >= blockMetadata.getLength()) {
        bytesToRead = (int)(blockMetadata.getLength() - offset);
      }
      byte[] record = new byte[bytesToRead];
      int bytesRead = 0;
      while (bytesRead < bytesToRead) {
        bytesRead += stream.read(record, bytesRead, bytesToRead - bytesRead);
      }
      entity.setUsedBytes(bytesRead);
      entity.setRecord(record);
      return entity;
    }
  }
}

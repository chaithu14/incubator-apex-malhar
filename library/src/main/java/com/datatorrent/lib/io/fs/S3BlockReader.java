/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.lib.io.fs;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;

import com.datatorrent.api.Context;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.FSSliceReader;
import com.datatorrent.lib.io.block.ReaderContext;

/**
 * S3BlockReader extends from BlockReader and serves the functionality of read objects and
 * parse Block metadata
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class S3BlockReader extends FSSliceReader
{
  protected transient String s3bucketUri;
  private String bucketName;
  private transient AmazonS3 s3Client;
  private String accessKey;
  private String secretKey;

  public S3BlockReader()
  {
    this.readerContext = new S3BlockReaderContext();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    s3bucketUri = fs.getScheme() + "://" + bucketName;
    s3Client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
    ((S3BlockReaderContext)readerContext).setBucketName(bucketName);
    ((S3BlockReaderContext)readerContext).setS3Client(s3Client);

  }

  /**
   * Extracts the bucket name from the given uri
   * @param s3uri s3 uri
   * @return name of the bucket
   */
  @VisibleForTesting
  protected static String extractBucket(String s3uri)
  {
    return s3uri.substring(s3uri.indexOf('@') + 1, s3uri.indexOf("/", s3uri.indexOf('@')));
  }

  protected static String extractAccessKey(String s3uri)
  {
    return s3uri.substring(s3uri.indexOf("://") + 3, s3uri.indexOf(':', s3uri.indexOf("://") + 3));
  }

  protected static String extractSecretKey(String s3uri)
  {
    return s3uri.substring(s3uri.indexOf(':', s3uri.indexOf("://") + 1) + 1, s3uri.indexOf('@'));
  }

  /**
   * Create the stream from the bucket uri and block path.
   * @param block block metadata
   * @return stream
   * @throws IOException
   */
  @Override
  protected FSDataInputStream setupStream(BlockMetadata.FileBlockMetadata block) throws IOException
  {
    String filePath = block.getFilePath();
    if (filePath.startsWith("/")) {
      filePath = filePath.substring(1);
    }
    ((S3BlockReaderContext)readerContext).setKey(filePath);
    return null;

  }

  /**
   * BlockReadeContext for reading S3 Blocks. Stream could't able be read the complete block.
   * This will wait till the block reads completely.
   */
  private static class S3BlockReaderContext extends ReaderContext.FixedBytesReaderContext<FSDataInputStream>
  {
    private transient AmazonS3 s3Client;
    private transient String bucketName;
    private String key;
    /**
     * S3 File systems doesn't read the specified block completely while using readFully API.
     * This will read small chunks continuously until will reach the specified block size.
     * @return the block entity
     * @throws IOException
     */
    @Override
    protected Entity readEntity() throws IOException
    {
      /*entity.clear();
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
      return entity;*/
      entity.clear();
      GetObjectRequest rangeObjectRequest = new GetObjectRequest(
          bucketName, key);
      rangeObjectRequest.setRange(offset, blockMetadata.getLength());
      S3Object objectPortion = s3Client.getObject(rangeObjectRequest);
      S3ObjectInputStream wrappedStream = objectPortion.getObjectContent();
      byte[] record = ByteStreams.toByteArray(wrappedStream);
      entity.setUsedBytes(record.length);
      entity.setRecord(record);
      return entity;

    }

    public String getKey()
    {
      return key;
    }

    public void setKey(String key)
    {
      this.key = key;
    }

    public AmazonS3 getS3Client()
    {
      return s3Client;
    }

    public void setS3Client(AmazonS3 s3Client)
    {
      this.s3Client = s3Client;
    }

    public String getBucketName()
    {
      return bucketName;
    }

    public void setBucketName(String bucketName)
    {
      this.bucketName = bucketName;
    }
  }

  /**
   * Get the S3 bucket name
   * @return bucket
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Set the bucket name
   * @param bucketName bucket name
   */
  public void setBucketName(String bucketName)
  {
    this.bucketName = bucketName;
  }

  public String getAccessKey()
  {
    return accessKey;
  }

  public void setAccessKey(String accessKey)
  {
    this.accessKey = accessKey;
  }

  public String getSecretKey()
  {
    return secretKey;
  }

  public void setSecretKey(String secretKey)
  {
    this.secretKey = secretKey;
  }
}

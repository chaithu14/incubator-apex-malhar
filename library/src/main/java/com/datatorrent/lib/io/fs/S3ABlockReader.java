package com.datatorrent.lib.io.fs;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.common.io.ByteStreams;

import com.datatorrent.api.Context;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.FSSliceReader;
import com.datatorrent.lib.io.block.ReaderContext;

public class S3ABlockReader extends FSSliceReader
{
  public static final String SCHEME = "DTS3";

  private transient String s3bucketUri;
  private String inputURI;
  private String accessKey;
  private String secretKey;
  private String bucketKey;
  private transient AmazonS3 s3Client;

  public S3ABlockReader()
  {
    this.readerContext = new S3BlockReaderContext();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    s3Client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
    ((S3BlockReaderContext)readerContext).setBucketName(bucketKey);
    ((S3BlockReaderContext)readerContext).setS3Client(s3Client);
  }

  /**
   * Create the stream from the bucket uri and block path.
   * @param block
   * @return
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
   * This will wait till the block reads completely <br/>
   */
  private static class S3BlockReaderContext extends ReaderContext.FixedBytesReaderContext<FSDataInputStream>
  {
    private transient AmazonS3 s3Client;
    private transient String bucketName;
    private String key;
    private static final Logger LOG = LoggerFactory.getLogger(S3BlockReaderContext.class);
    @Override
    protected Entity readEntity() throws IOException
    {
      entity.clear();
      GetObjectRequest rangeObjectRequest = new GetObjectRequest(
          bucketName, key);
      LOG.info("BucketKey: {} -> {}",bucketName, key);
      rangeObjectRequest.setRange(offset, blockMetadata.getLength());
      S3Object objectPortion = s3Client.getObject(rangeObjectRequest);
      S3ObjectInputStream wrappedStream = objectPortion.getObjectContent();
      byte[] record = ByteStreams.toByteArray(wrappedStream);
      entity.setUsedBytes(record.length);
      entity.setRecord(record);
      LOG.info("End of readEntity");
      return entity;
    }

    public AmazonS3 getS3Client()
    {
      return s3Client;
    }

    public void setS3Client(AmazonS3 s3Client)
    {
      this.s3Client = s3Client;
    }

    public String getKey()
    {
      return key;
    }

    public void setKey(String key)
    {
      this.key = key;
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
   * Returns the inputURI
   * @return
   */
  public String getInputURI()
  {
    return inputURI;
  }

  /**
   * Sets the inputURI which is in the form of s3://accessKey:secretKey@bucketName/
   * @param inputURI inputURI
   */
  public void setInputURI(String inputURI)
  {
    this.inputURI = inputURI;
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

  public String getBucketKey()
  {
    return bucketKey;
  }

  public void setBucketKey(String bucketKey)
  {
    this.bucketKey = bucketKey;
  }
}

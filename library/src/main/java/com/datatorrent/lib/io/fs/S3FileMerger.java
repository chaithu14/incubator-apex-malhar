package com.datatorrent.lib.io.fs;

import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.esotericsoftware.kryo.NotNull;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;

public class S3FileMerger extends FileMerger
{
  private static final Logger logger = LoggerFactory.getLogger(S3FileMerger.class);
  protected static final String S3_TMP_DIR = "S3TmpFiles";

  protected transient Configuration conf;
  @NotNull
  private String accessKey;
  @NotNull
  private String secretKey;
  @NotNull
  private String bucketName;
  @NotNull
  private String directoryName;
  protected transient AmazonS3 s3client;

  @Override
  public void setup(Context.OperatorContext context)
  {
    s3client = new AmazonS3Client(new BasicAWSCredentials(accessKey,secretKey));
    filePath = context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + S3_TMP_DIR;
    super.setup(context);
  }

  /**
   * S3 supports rename functionality. But, it effects the performance. Because, rename does the following steps:
   * 1) Downloads the object 2) Remove the object 3) Uploads the object with final name.
   * Instead of writing to temp file, it writes to final file. So, temp path is pointing to final path.
   * @param outFileMetadata File Meta info of output file
   * @return
   * @throws IOException
   * @throws BlockNotFoundException
   */
  @Override
  protected OutputStream writeTempOutputFile(Synchronizer.OutputFileMetadata outFileMetadata) throws IOException, BlockNotFoundException
  {
    tempOutFilePath = new Path(filePath, outFileMetadata.getStitchedFileRelativePath());
    return super.writeTempOutputFile(outFileMetadata);
  }

  /**
   * WriteTempOutputFile writes to file path, so need to do any functionality in moveToFinalFile.
   * @param outFileMetadata
   * @throws IOException
   */
  @Override
  protected void moveToFinalFile(Synchronizer.OutputFileMetadata outFileMetadata) throws IOException
  {
    try {
      FSDataInputStream fsinput = appFS.open(tempOutFilePath);
      ObjectMetadata omd = new ObjectMetadata();
      omd.setContentLength(outFileMetadata.getFileLength());
      String keyName = directoryName + Path.SEPARATOR + outFileMetadata.getStitchedFileRelativePath();
      s3client.putObject(new PutObjectRequest(bucketName, keyName, fsinput, omd));
    } catch (IOException e) {
      logger.error("Unable to create Stream: {}", e.getMessage());
    }
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

  public String getBucketName()
  {
    return bucketName;
  }

  public void setBucketName(String bucketName)
  {
    this.bucketName = bucketName;
  }

  public String getDirectoryName()
  {
    return directoryName;
  }

  public void setDirectoryName(String directoryName)
  {
    this.directoryName = directoryName;
  }
}

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
package org.apache.apex.malhar.lib.fs.s3output;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.fs.FSInputModule;

public class S3OutputModuleAppTest
{
  private static Logger LOG = LoggerFactory.getLogger(S3OutputModuleAppTest.class);
  private String accessKey = "*****************";
  private String secretKey = "*****************";
  private String endPoint;

  private String input1Dir = "*******************";
  private String input2Dir;
  static String outputDir = "fs2s3output";
  private StreamingApplication app;
  private static final String FILE = "file.txt";
  private static final String FILE_DATA = "File one data. Name of file is file.txt";

  public static class TestMeta extends TestWatcher
  {
    public AmazonS3 client;
    public String bucketKey;
    public String keyName;
    public String baseDirectory;
    public File singleBlockFile;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.bucketKey = new String("target-" + description.getMethodName()).toLowerCase();
      this.baseDirectory = "target/" + description.getClassName() + "/" + description.getMethodName();
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);

    }
  }

  @Rule
  public S3OutputModuleAppTest.TestMeta testMeta = new S3OutputModuleAppTest.TestMeta();

  @Before
  public void setup() throws Exception
  {
    testMeta.client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
    if (endPoint != null) {
      testMeta.client.setEndpoint(endPoint);
    }
    testMeta.client.createBucket(testMeta.bucketKey);
    testMeta.keyName = outputDir + Path.getPathWithoutSchemeAndAuthority(new Path(input1Dir));
    input2Dir = testMeta.baseDirectory + File.separator + "input";
    testMeta.singleBlockFile = new File(input2Dir + File.separator + FILE);
    FileUtils.writeStringToFile(testMeta.singleBlockFile, FILE_DATA);
  }

  @After
  public void tearDown() throws IOException
  {
    FileUtils.deleteDirectory(new File(input2Dir));
    testMeta.client.deleteBucket(testMeta.bucketKey);
  }

  @Test
  public void testMultiPartUploadApplication() throws Exception
  {
    app = new S3OutputModuleAppTest.Application();
    Configuration conf = new Configuration();
    conf.set("dt.operator.HDFSInputModule.prop.files", input1Dir);
    conf.set("dt.operator.HDFSInputModule.prop.blockSize", "5242880");
    conf.set("dt.operator.HDFSInputModule.prop.blocksThreshold", "1");
    conf.set("dt.attr.CHECKPOINT_WINDOW_COUNT","20");

    conf.set("dt.operator.S3OutputModule.prop.accessKey", accessKey);
    conf.set("dt.operator.S3OutputModule.prop.secretAccessKey", secretKey);
    if (endPoint != null) {
      conf.set("dt.operator.S3OutputModule.prop.endPoint", endPoint);
    }
    conf.set("dt.operator.S3OutputModule.prop.bucketName", testMeta.bucketKey);
    conf.set("dt.operator.S3OutputModule.prop.outputDirectoryPath", outputDir);

    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);
    lc.runAsync();

    long now = System.currentTimeMillis();
    while (IsS3ObjectExists(testMeta.keyName, testMeta.bucketKey, testMeta.client) && System.currentTimeMillis() - now < 20000) {
      Thread.sleep(500);
      LOG.info("Waiting for {}", outputDir);
    }

    Thread.sleep(50000);
    lc.shutdown();
    Assert.assertEquals(true, IsS3ObjectExists(testMeta.keyName, testMeta.bucketKey, testMeta.client));
  }

  @Test
  public void testSinglePartUploadApplication() throws Exception
  {
    testMeta.keyName = outputDir + testMeta.singleBlockFile.getAbsolutePath();
    app = new S3OutputModuleAppTest.Application();
    Configuration conf = new Configuration();
    conf.set("dt.operator.HDFSInputModule.prop.files", input2Dir);
    conf.set("dt.operator.HDFSInputModule.prop.blockSize", "5242880");
    conf.set("dt.operator.HDFSInputModule.prop.blocksThreshold", "1");
    conf.set("dt.attr.CHECKPOINT_WINDOW_COUNT","20");

    conf.set("dt.operator.S3OutputModule.prop.accessKey", accessKey);
    conf.set("dt.operator.S3OutputModule.prop.secretAccessKey", secretKey);
    if (endPoint != null) {
      conf.set("dt.operator.S3OutputModule.prop.endPoint", endPoint);
    }
    conf.set("dt.operator.S3OutputModule.prop.bucketName", testMeta.bucketKey);
    conf.set("dt.operator.S3OutputModule.prop.outputDirectoryPath", outputDir);

    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);
    lc.runAsync();

    long now = System.currentTimeMillis();
    while (IsS3ObjectExists(testMeta.keyName, testMeta.bucketKey, testMeta.client) && System.currentTimeMillis() - now < 20000) {
      Thread.sleep(500);
      LOG.info("Waiting for {}", outputDir);
    }

    Thread.sleep(20000);
    lc.shutdown();
    Assert.assertEquals(true, IsS3ObjectExists(testMeta.keyName, testMeta.bucketKey, testMeta.client));
  }

  private static class Application implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      FSInputModule inputModule = dag.addModule("HDFSInputModule", new FSInputModule());
      S3OutputModule outputModule = dag.addModule("S3OutputModule", new S3OutputModule());

      dag.addStream("FileMetaData", inputModule.filesMetadataOutput, outputModule.filesMetadataInput);
      dag.addStream("BlocksMetaData", inputModule.blocksMetadataOutput, outputModule.blocksMetadataInput)
        .setLocality(DAG.Locality.CONTAINER_LOCAL);
      dag.addStream("BlocksData", inputModule.messages, outputModule.blockData).setLocality(DAG.Locality.CONTAINER_LOCAL);

    }
  }

  public static boolean IsS3ObjectExists(String path, String bucketName, AmazonS3 client)
  {
    try {
      client.getObjectMetadata(bucketName, path);
    } catch (AmazonServiceException e) {
      return false;
    }
    return true;
  }
}

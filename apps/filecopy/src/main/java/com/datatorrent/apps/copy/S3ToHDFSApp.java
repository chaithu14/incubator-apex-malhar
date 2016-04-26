package com.datatorrent.apps.copy;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.fs.HDFSFileCopyModule;
import com.datatorrent.lib.io.fs.S3InputModule;

/**
 * Application for HDFS to HDFS file copy
 */
@ApplicationAnnotation(name = "S3FileCopySampleApp")
public class S3ToHDFSApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    S3InputModule inputModule = dag.addModule("HDFSInputModule", new S3InputModule());
    HDFSFileCopyModule outputModule = dag.addModule("FileCopyModule", new HDFSFileCopyModule());

    dag.addStream("FileMetaData", inputModule.filesMetadataOutput, outputModule.filesMetadataInput);
    dag.addStream("BlocksMetaData", inputModule.blocksMetadataOutput, outputModule.blocksMetadataInput)
      .setLocality(DAG.Locality.THREAD_LOCAL);
    dag.addStream("BlocksData", inputModule.messages, outputModule.blockData).setLocality(DAG.Locality.THREAD_LOCAL);

  }
}

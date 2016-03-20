package com.datatorrent.lib.io.fs;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class S3FileMerger extends FileMerger
{
  protected static final String S3_TMP_DIR = "s3";

  protected transient Configuration conf;

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
  }
}

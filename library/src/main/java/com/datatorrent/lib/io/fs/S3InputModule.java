package com.datatorrent.lib.io.fs;

import com.datatorrent.lib.io.block.BlockReader;

/**
 * S3InputModule is used to read files/list of files (or directory) from S3 bucket. <br/>
 * Module emits, <br/>
 * 1. FileMetadata 2. BlockMetadata 3. Block Bytes.<br/><br/>
 * The module reads data in parallel, following parameters can be configured<br/>
 * 1. files: list of file(s)/directories to read<br/>
 * 2. filePatternRegularExp: Files names matching given regex will be read<br/>
 * 3. scanIntervalMillis: interval between two scans to discover new files in input directory<br/>
 * 4. recursive: if scan recursively input directories<br/>
 * 5. blockSize: block size used to read input blocks of file<br/>
 * 6. readersCount: count of readers to read input file<br/>
 * 7. sequencialFileRead: If emit file blocks in sequence?
 */

public class S3InputModule extends FSInputModule
{
  @Override
  public FSFileSplitter createFileSplitter()
  {
    return new S3FileSplitter();
  }

  @Override
  public BlockReader createBlockReader()
  {
    return new S3BlockReader();
  }
}

package com.datatorrent.lib.io.fs;

public class S3FileCopyOutputModule extends FSCopyOutputModule
{
  @Override
  public BlockWriter createBlockWriter()
  {
    return new BlockWriter();
  }

  @Override
  public Synchronizer createSynchronizer()
  {
    return new Synchronizer();
  }

  @Override
  public FileMerger createFileMerger()
  {
    return new S3FileMerger();
  }
}

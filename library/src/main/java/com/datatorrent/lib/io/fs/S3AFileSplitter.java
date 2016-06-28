package com.datatorrent.lib.io.fs;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.datatorrent.netlet.util.DTThrowable;

public class S3AFileSplitter extends FileSplitterInput
{
  public S3AFileSplitter()
  {
    super();
    super.setScanner(new S3Scanner());
  }

  public static class S3Scanner extends TimeBasedDirectoryScanner
  {
    private transient AmazonS3 s3Client;
    private transient String bucketName;
    private transient volatile boolean running;

    public S3Scanner()
    {
      super();
    }

    @Override
    protected FileSystem getFSInstance() throws IOException
    {
      return null;
    }

    @Override
    public void run()
    {
      running = true;
      try {
        while (running) {
          if ((isTrigger() || (System.currentTimeMillis() - getScanIntervalMillis() >= lastScanMillis)) && isIterationCompleted()) {
            setTrigger(false);
            lastScannedInfo = null;
            numDiscoveredPerIteration = 0;
            for (String afile : files) {
              Path filePath = new Path(afile);
              //LOG.debug("Scan started for input {}", filePath);
              Map<String, Long> lastModifiedTimesForInputDir = referenceTimes.get(filePath.toString());
              scan(filePath, null, lastModifiedTimesForInputDir);
            }
            scanIterationComplete();
          } else {
            Thread.sleep(sleepMillis);
          }
        }
      } catch (Throwable throwable) {
        //LOG.error("service", throwable);
        running = false;
        atomicThrowable.set(throwable);
        DTThrowable.rethrow(throwable);
      }
    }

    private void scan(Path filePath, Path rootPath, Map<String, Long> lastModifiedTimesForInputDir)
    {
      String parentPathStr = filePath.toString();
      ListObjectsRequest request = new ListObjectsRequest();
      request.setBucketName(bucketName);
      request.setPrefix(filePath.toString());
      request.setDelimiter("/");

      LOG.debug("scan {}", parentPathStr);

      ObjectListing objects = s3Client.listObjects(request);
      ObjectStatus parentStatus = createObjectStatus(parentPathStr);
      List<S3ObjectSummary> childStatuses = objects.getObjectSummaries();

      if (childStatuses.size() == 0 && rootPath == null && (lastModifiedTimesForInputDir == null || lastModifiedTimesForInputDir.get(parentPathStr) == null)) { // empty input directory copy as is
        ScannedFileInfo info = new ScannedFileInfo(null, filePath.toString(), parentStatus.getModificationTime());
        processDiscoveredFile(info);
      }

      for (S3ObjectSummary childSummary : childStatuses) {
        String childPathStr = childSummary.getKey();

        ListObjectsRequest childRequest = new ListObjectsRequest();
        childRequest.setBucketName(bucketName);
        childRequest.setPrefix(childPathStr);
        childRequest.setDelimiter("/");

        ObjectListing childObjects = s3Client.listObjects(childRequest);
        ObjectStatus childStatus = createObjectStatus(childPathStr);

        if (childStatus.isDirectory() && isRecursive()) {
          //addToDiscoveredFiles(rootPath, parentStatus, childStatus, lastModifiedTimesForInputDir);
          scan(new Path(childPathStr), rootPath == null ? filePath : rootPath, lastModifiedTimesForInputDir);
        } else if (acceptFile(childPathStr)) {
          //addToDiscoveredFiles(rootPath, parentStatus, childStatus, lastModifiedTimesForInputDir);
        } else {
          // don't look at it again
          ignoredFiles.add(childPathStr);
        }
      }
    }

    private void addToDiscoveredFiles(Path rootPath, ObjectStatus parentStatus, ObjectStatus childStatus,
        Map<String, Long> lastModifiedTimesForInputDir) throws IOException
    {
      String childPath = childStatus.getPath();
      // Directory by now is scanned forcibly. Now check for whether file/directory needs to be added to discoveredFiles.
      Long oldModificationTime = null;
      if (lastModifiedTimesForInputDir != null) {
        oldModificationTime = lastModifiedTimesForInputDir.get(childPath);
      }

      if (skipFile(new Path(childPath), childStatus.getModificationTime(), oldModificationTime) || // Skip dir or file if no timestamp modification
          (childStatus.isDirectory() && (oldModificationTime != null))) { // If timestamp modified but if its a directory and already present in map, then skip.
        return;
      }

      if (ignoredFiles.contains(childPath)) {
        return;
      }

      ScannedFileInfo info = createScannedFileInfo(new Path(parentStatus.getPath()), parentStatus,
          new Path(childPath), childStatus, rootPath);

      LOG.debug("Processing file: " + info.getFilePath());
      processDiscoveredFile(info);
    }

    protected ScannedFileInfo createScannedFileInfo(Path parentPath, ObjectStatus parentStatus, Path childPath,
        ObjectStatus childStatus, Path rootPath)
    {
      ScannedFileInfo info;
      if (rootPath == null) {
        info = parentStatus.isDirectory() ?
          new ScannedFileInfo(parentPath.toUri().getPath(), childPath.getName(), childStatus.getModificationTime()) :
          new ScannedFileInfo(null, childPath.toUri().getPath(), childStatus.getModificationTime());
      } else {
        URI relativeChildURI = rootPath.toUri().relativize(childPath.toUri());
        info = new ScannedFileInfo(rootPath.toUri().getPath(), relativeChildURI.getPath(),
          childStatus.getModificationTime());
      }
      return info;
    }


    ObjectStatus createObjectStatus(String key)
    {
      ObjectStatus os = new ObjectStatus();
      if (!key.isEmpty()) {
        try {
          ObjectMetadata meta = s3Client.getObjectMetadata(bucketName, key);
          LOG.info("getFileStatus - 1:");
          if (IsDirectory(key, meta.getContentLength())) {
            os.setDirectory(true);
          }
          os.setPath(key);
          os.setModificationTime(meta.getLastModified().getTime());
        } catch (AmazonServiceException e) {
          if (e.getStatusCode() != 404) {
            throw e;
          }
        } catch (AmazonClientException e) {
          throw e;
        }

        if (!key.endsWith("/")) {
          try {
            String newKey = key + "/";
            ObjectMetadata meta = s3Client.getObjectMetadata(bucketName, key);
            if (IsDirectory(newKey, meta.getContentLength())) {
              os.setDirectory(true);
            }
            os.setPath(key);
            os.setModificationTime(meta.getLastModified().getTime());
          } catch (AmazonServiceException e) {
            if (e.getStatusCode() != 404) {
              throw e;
            }
          } catch (AmazonClientException e) {
            throw e;
          }
        }
      }
      return os;
    }

    boolean IsDirectory(String key, long length)
    {
      return !key.isEmpty() && key.charAt(key.length() - 1) == '/' && length == 0;
    }
  }

  public static class ObjectStatus
  {
    String path;
    boolean directory;
    long modificationTime;

    public ObjectStatus()
    {
      directory = false;
    }

    public ObjectStatus(String path, boolean directory, long modificationTime)
    {
      this.path = path;
      this.directory = directory;
      this.modificationTime = modificationTime;
    }

    public String getPath()
    {
      return path;
    }

    public void setPath(String path)
    {
      this.path = path;
    }

    public boolean isDirectory()
    {
      return directory;
    }

    public void setDirectory(boolean directory)
    {
      this.directory = directory;
    }

    public long getModificationTime()
    {
      return modificationTime;
    }

    public void setModificationTime(long modificationTime)
    {
      this.modificationTime = modificationTime;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(S3AFileSplitter.class);
}

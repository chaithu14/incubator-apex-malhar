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
import org.apache.hadoop.fs.FileSystem;
import com.datatorrent.api.Context;

/**
 * S3FileSplitter extends from FSFileSplitter and dissables the parallel read functionality
 * explicitly in case of s3 or s3n schemes.
 * Parallel read will work only if the scheme is "s3a" and the Hadoop version is 2.7+.
 * Parallel read doesn't work in the case of the scheme is "s3n/s3". In this case, this operator explicitly
 * disables the parallel read functionality.
 */
public class S3FileSplitter extends FSFileSplitter
{
  public S3FileSplitter()
  {
    super();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    FSScanner scanner = (FSScanner)getScanner();
    try {
      FileSystem fs = scanner.getFSInstance();
      String scheme = fs.getScheme();
      // Parallel read doesn't support incase of scheme is s3 (or) s3n.
      if(scheme.equals("s3") || scheme.equals("s3n")) {
        setSequencialFileRead(true);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

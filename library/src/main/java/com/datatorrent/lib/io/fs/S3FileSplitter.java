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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;

import com.datatorrent.api.Context;

public class S3FileSplitter extends FSFileSplitter
{
  private static final Logger LOG = LoggerFactory.getLogger(S3FileSplitter.class);
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
      if(scheme.equals("s3") || scheme.equals("s3n")) {
        setSequencialFileRead(true);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

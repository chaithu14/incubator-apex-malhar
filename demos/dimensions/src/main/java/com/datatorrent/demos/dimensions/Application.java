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
package com.datatorrent.demos.dimensions;

import org.joda.time.Duration;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.join.InMemoryStore;
import com.datatorrent.lib.join.POJOJoinOperator;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name="JoinDemo")
public class Application implements StreamingApplication
{
  public static class CollectorModule extends BaseOperator
  {
    public final transient DefaultInputPort<Object> inputPort = new DefaultInputPort<Object>()
    {

      @Override
      public void process(Object arg0)
      {
      }
    };

  }


  @Override public void populateDAG(DAG dag, Configuration conf)
    {
      long timeInterval = 60000 * 10;
      long bucketTime = 60000 * 1;
      JsonSalesGenerator input = dag.addOperator("Input", JsonSalesGenerator.class);
      input.setAddProductCategory(false);
      input.setMaxTuplesPerWindow(100);
      input.setTuplesPerWindowDeviation(0);
      input.setTimeInterval(timeInterval);
      input.setTimeBucket(bucketTime);

      JsonProductGenerator input2 = dag.addOperator("Prodcut", JsonProductGenerator.class);
      input2.setMaxTuplesPerWindow(100);
      input2.setTuplesPerWindowDeviation(0);
      input2.setTimeInterval(timeInterval);
      input2.setTimeBucket(bucketTime);

      POJOJoinOperator joinOper = dag.addOperator("Join", new POJOJoinOperator());
      ManagedStateStore lStore = new ManagedStateStore();
      lStore.getTimeBucketAssigner().setBucketSpan(Duration.millis(bucketTime));
      lStore.getTimeBucketAssigner().setExpireBefore(Duration.millis(timeInterval));
      ManagedStateStore rStore = new ManagedStateStore();
      rStore.getTimeBucketAssigner().setBucketSpan(Duration.millis(bucketTime));
      lStore.getTimeBucketAssigner().setExpireBefore(Duration.millis(timeInterval));
      /*InMemoryStore lStore = new InMemoryStore(timeInterval, (int) bucketTime);
      InMemoryStore rStore = new InMemoryStore(timeInterval, (int) bucketTime);*/
      joinOper.setLeftStore(lStore);
      joinOper.setRightStore(rStore);
      joinOper.setIncludeFields("timestamp,customerId,productId,regionId,amount;productCategory");
      joinOper.setKeyFields("productId,productId");
      //joinOper.setTimeFields("timestamp,timestamp");

      joinOper.setOutputClass("com.datatorrent.demos.dimensions.SalesEvent");
      //CollectorModule console = dag.addOperator("Console", new CollectorModule());
      ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());
      dag.addStream("SalesInput", input.outputPort, joinOper.input1);
      dag.addStream("JsonProductStream", input2.outputPort, joinOper.input2);
      dag.addStream("Output", joinOper.outputPort, console.input).setLocality(DAG.Locality.CONTAINER_LOCAL);

    }
}

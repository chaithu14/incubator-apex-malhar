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
package org.apache.apex.malhar.kafka;

import java.io.File;
import java.util.Properties;

import org.slf4j.LoggerFactory;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

/**
 * This is a base class setup/clean Kafka testing environment for all the input/output test If it's a multipartition
 * test, this class creates 2 kafka partitions
 */
public class KafkaOperatorTestBase extends AbstractKafkaOperatorTestBase
{
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaOperatorTestBase.class);
  // since Kafka 0.8 use KafkaServerStatble instead of KafkaServer

  // multiple brokers in multiple cluster
  private static KafkaServerStartable[] broker = new KafkaServerStartable[2];

  private static final String[] kafkadir = new String[]{"kafka-server-data/1", "kafka-server-data/2"};

  public void startKafkaServer(int clusterid, int brokerid)
  {
    Properties props = new Properties();
    props.setProperty("broker.id", "" + clusterid * 10 + brokerid);
    props.setProperty("log.dirs", new File(baseDir, kafkadir[clusterid]).toString());
    props.setProperty("zookeeper.connect", "localhost:" + TEST_ZOOKEEPER_PORT[clusterid]);
    props.setProperty("port", "" + TEST_KAFKA_BROKER_PORT[clusterid]);
    props.setProperty("default.replication.factor", "1");
    // set this to 50000 to boost the performance so most test data are in memory before flush to disk
    props.setProperty("log.flush.interval.messages", "50000");

    broker[clusterid] = new KafkaServerStartable(new KafkaConfig(props));
    broker[clusterid].startup();

  }

  public void stopKafkaServer()
  {
    /*for (int i = 0; i < broker.length; i++) {
      for (int j = 0; j < broker[i].length; j++) {
        if (broker[i][j] != null) {
          logger.warn("-- stopKafkaServer :{} -> {}", i, j);
          broker[i][j].shutdown();
          //broker[i][j].awaitShutdown();
          broker[i][j] = null;
          logger.warn("-- ENd of stopKafkaServer :{} -> {}", i, j);
        }
      }
    }*/
    for (int i = 0; i < broker.length; i++) {
      if (broker[i] != null) {
        logger.warn("-- stopKafkaServer :{} -> {}", i);
        broker[i].shutdown();
        //broker[i][j].awaitShutdown();
        broker[i] = null;
        logger.warn("-- ENd of stopKafkaServer :{} -> {}", i);
      }
    }
  }
}

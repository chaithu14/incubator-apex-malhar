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
import java.net.InetSocketAddress;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

/**
 * A bunch of test to verify the input operator will be automatically partitioned
 * per kafka partition This test is launching its
 * own Kafka cluster.
 */
@RunWith(Parameterized.class)
public class KafkaInputOperatorTest extends AbstractKafkaInputOperatorTest
{
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaInputOperatorTest.class);

  public KafkaInputOperatorTest(boolean hasMultiCluster, boolean hasMultiPartition, String partition)
  {
    super(hasMultiCluster, hasMultiPartition, partition);
  }

  KafkaSinglePortInputOperator node;

  @Override
  public AbstractKafkaInputOperator createAndAddKafkaOpToDAG(DAG dag, String testName)
  {
    node = dag.addOperator(testName, new KafkaSinglePortInputOperator());
    return node;
  }

  @Override
  public DefaultOutputPort getOutputPortOfKafka()
  {
    return node.outputPort;
  }

  public static void startZookeeper(final int clusterId)
  {
    try {
      logger.warn("start Zookeeper - 1");
      int numConnections = 100;
      int tickTime = 2000;
      File dir = new File(baseDir, zkdir[clusterId]);
      logger.info("start Zookeeper - 2: {}", dir.getPath());
      zkServer[clusterId] = new TestZookeeperServer(dir, dir, tickTime);
      logger.warn("start Zookeeper - 3: {} -> {}", clusterId, dir.getPath());
      zkFactory[clusterId] = new NIOServerCnxnFactory();
      logger.warn("start Zookeeper - 4: {} -> {}", clusterId, dir.getPath());
      zkFactory[clusterId].configure(new InetSocketAddress(TEST_ZOOKEEPER_PORT[clusterId]), numConnections);
      logger.warn("start Zookeeper - 5: {} -> {}", clusterId, dir.getPath());
      zkFactory[clusterId].startup(zkServer[clusterId]); // start the zookeeper server.
      logger.warn("start Zookeeper - 6: {} -> {}", clusterId, dir.getPath());
      Thread.sleep(2000);
      //kserver.startup();
    } catch (Exception ex) {
      logger.error(ex.getLocalizedMessage());
    }
  }

  public static void startKafkaServer(int clusterid, int brokerid)
  {
    Properties props = new Properties();
    props.setProperty("broker.id", "" + clusterid * 10 + brokerid);
    props.setProperty("log.dirs", new File(baseDir, kafkadir[clusterid][brokerid]).toString());
    props.setProperty("zookeeper.connect", "localhost:" + TEST_ZOOKEEPER_PORT[clusterid]);
    props.setProperty("port", "" + TEST_KAFKA_BROKER_PORT[clusterid][brokerid]);
    props.setProperty("default.replication.factor", "1");
    // set this to 50000 to boost the performance so most test data are in memory before flush to disk
    props.setProperty("log.flush.interval.messages", "50000");

    logger.warn("startKafkaServer: clusterid {} : {}", clusterid, brokerid);
    broker[clusterid][brokerid] = new KafkaServerStartable(new KafkaConfig(props));
    logger.warn("startKafkaServer: clusterid {} , brokerid: {}", clusterid, brokerid);
    broker[clusterid][brokerid].startup();
    logger.warn("startKafkaServer.startup(): clusterid {} , brokerid: {}", clusterid, brokerid);
  }

  public static void startKafkaServer()
  {
    FileUtils.deleteQuietly(new File(baseDir, kafkaBaseDir));
    logger.warn("start kafka server - 1: {} -> {}", baseDir, kafkaBaseDir);
    //boolean[][] startable = new boolean[][] { new boolean[] { true, hasMultiPartition },
    //  new boolean[] { hasMultiCluster, hasMultiCluster && hasMultiPartition } };
    startKafkaServer(0, 0);
    logger.warn("start kafka server - 2: {} -> {}", baseDir, kafkaBaseDir);
    startKafkaServer(0, 1);
    logger.warn("start kafka server - 3: {} -> {}", baseDir, kafkaBaseDir);
    startKafkaServer(1, 0);
    startKafkaServer(1, 1);

    // startup is asynch operation. wait 2 sec for server to startup

  }

  public static void stopZookeeper()
  {
    for (ZooKeeperServer zs : zkServer) {
      if (zs != null) {
        zs.shutdown();
      }
    }

    for (ServerCnxnFactory zkf : zkFactory) {
      if (zkf != null) {
        zkf.closeAll();
        zkf.shutdown();
      }
    }
    zkServer = new ZooKeeperServer[2];
    zkFactory = new ServerCnxnFactory[2];
  }

  public static void stopKafkaServer()
  {
    for (int i = 0; i < broker.length; i++) {
      for (int j = 0; j < broker[i].length; j++) {
        if (broker[i][j] != null) {
          broker[i][j].shutdown();
          broker[i][j].awaitShutdown();
          broker[i][j] = null;
        }
      }
    }
  }

  public static void startZookeeper()
  {
    FileUtils.deleteQuietly(new File(baseDir, zkBaseDir));
    startZookeeper(0);
    startZookeeper(1);
  }

  @BeforeClass
  public static void beforeTest()
  {
    try {
      logger.warn("--------- beforeTest--------");
      startZookeeper();
      startKafkaServer();
    } catch (java.nio.channels.CancelledKeyException ex) {
      logger.debug("LSHIL {}", ex.getLocalizedMessage());
    }
  }

  @AfterClass
  public static void afterTest()
  {
    try {
      stopKafkaServer();
      stopZookeeper();
    } catch (Exception ex) {
      logger.debug("LSHIL {}", ex.getLocalizedMessage());
    }
  }

}

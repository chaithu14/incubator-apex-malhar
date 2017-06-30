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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import kafka.admin.TopicCommand;
import kafka.utils.ZkUtils;

/**
 * This is a base class setup/clean Kafka testing environment for all the input/output test If it's a multipartition
 * test, this class creates 2 kafka partitions
 */
public abstract class AbstractKafkaOperatorTestBase
{
  public static final String END_TUPLE = "END_TUPLE";
  public static final int[] TEST_ZOOKEEPER_PORT;
  public static final int[] TEST_KAFKA_BROKER_PORT;
  public static final String TEST_TOPIC = "testtopic";

  // get available ports
  static {
    ServerSocket[] listeners = new ServerSocket[4];
    int[] p = new int[4];

    try {
      for (int i = 0; i < 4; i++) {
        listeners[i] = new ServerSocket(0);
        p[i] = listeners[i].getLocalPort();
      }

      for (int i = 0; i < 4; i++) {
        listeners[i].close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    TEST_ZOOKEEPER_PORT = new int[]{p[0], p[1]};
    TEST_KAFKA_BROKER_PORT = new int[]{p[2], p[3]};
  }

  static final org.slf4j.Logger logger = LoggerFactory.getLogger(AbstractKafkaOperatorTestBase.class);
  // since Kafka 0.8 use KafkaServerStatble instead of KafkaServer

  // multiple cluster
  private static ServerCnxnFactory[] zkFactory = new ServerCnxnFactory[2];

  private static ZooKeeperServer[] zkServer = new ZooKeeperServer[2];

  public static String baseDir = "target";

  private static final String zkBaseDir = "zookeeper-server-data";
  private static final String kafkaBaseDir = "kafka-server-data";
  private static final String[] zkdir = new String[]{"zookeeper-server-data/1", "zookeeper-server-data/2"};
  protected boolean hasMultiPartition = false;
  protected boolean hasMultiCluster = false;

  public static void startZookeeper(final int clusterId)
  {
    try {
      int numConnections = 100;
      int tickTime = 2000;
      File dir = new File(baseDir, zkdir[clusterId]);

      zkServer[clusterId] = new ZooKeeperServer(dir, dir, tickTime);
      zkFactory[clusterId] = NIOServerCnxnFactory.createFactory();
      zkFactory[clusterId].configure(new InetSocketAddress("localhost", TEST_ZOOKEEPER_PORT[clusterId]), numConnections);

      zkFactory[clusterId].startup(zkServer[clusterId]); // start the zookeeper server.
      Thread.sleep(2000);
      //kserver.startup();
    } catch (Exception ex) {
      logger.error(ex.getLocalizedMessage());
    }
  }

  public void stopZookeeper()
  {
    /*for (ZooKeeperServer zs : zkServer) {
      if (zs != null) {
        zs.shutdown();
      }
    }*/

    for (ServerCnxnFactory zkf : zkFactory) {
      if (zkf != null) {
        logger.warn("-- stopZookeeper : {}", zkf.toString());
        //zkf.closeAll();
        zkf.shutdown();
      }
      logger.warn("-- End of stopZookeeper :");
    }
    zkServer = new ZooKeeperServer[2];
    zkFactory = new ServerCnxnFactory[2];
  }

  public abstract void startKafkaServer(int clusterid, int brokerid);

  public void startKafkaServer()
  {
    FileUtils.deleteQuietly(new File(baseDir, kafkaBaseDir));
    //boolean[][] startable = new boolean[][] { new boolean[] { true, hasMultiPartition },
    //  new boolean[] { hasMultiCluster, hasMultiCluster && hasMultiPartition } };
    startKafkaServer(0, 0);
    startKafkaServer(1, 0);

    // startup is asynch operation. wait 2 sec for server to startup
  }

  public void start()
  {
    try {
      startZookeeper();
      startKafkaServer();
    } catch (java.nio.channels.CancelledKeyException ex) {
      logger.debug("LSHIL {}", ex.getLocalizedMessage());
    }
  }

  public void startZookeeper()
  {
    FileUtils.deleteQuietly(new File(baseDir, zkBaseDir));
    startZookeeper(0);
    startZookeeper(1);
  }

  public void createTopic(int clusterid, String topicName)
  {
    String[] args = new String[9];
    args[0] = "--zookeeper";
    args[1] = "localhost:" + TEST_ZOOKEEPER_PORT[clusterid];
    args[2] = "--replication-factor";
    args[3] = "1";
    args[4] = "--partitions";
    if (hasMultiPartition) {
      args[5] = "2";
    } else {
      args[5] = "1";
    }
    args[6] = "--topic";
    args[7] = topicName;
    args[8] = "--create";

    ZkUtils zu = ZkUtils.apply("localhost:" + TEST_ZOOKEEPER_PORT[clusterid], 30000, 30000, false);
    TopicCommand.createTopic(zu, new TopicCommand.TopicCommandOptions(args));

  }

  public abstract void stopKafkaServer();

  public void stop()
  {
    try {
      logger.warn("-- after test: ");
      stopKafkaServer();
      stopZookeeper();
      Thread.sleep(1000);
    } catch (Exception ex) {
      logger.debug("LSHIL {}", ex.getLocalizedMessage());
    }
  }

  public void setHasMultiPartition(boolean hasMultiPartition)
  {
    this.hasMultiPartition = hasMultiPartition;
  }

  public void setHasMultiCluster(boolean hasMultiCluster)
  {
    this.hasMultiCluster = hasMultiCluster;
  }
}

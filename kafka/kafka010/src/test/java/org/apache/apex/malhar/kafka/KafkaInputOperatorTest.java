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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;

/**
 * A bunch of test to verify the input operator will be automatically partitioned
 * per kafka partition This test is launching its
 * own Kafka cluster.
 */
@RunWith(Parameterized.class)
public class KafkaInputOperatorTest extends AbstractKafkaInputOperatorTest
{

  public KafkaInputOperatorTest(boolean hasMultiCluster, boolean hasMultiPartition, String partition)
  {
    super(hasMultiCluster, hasMultiPartition, partition);
  }

  @Override
  public AbstractKafkaTestProducer createProducer(String topic, boolean isMultiPartition, boolean isMultiCluster)
  {
    return new KafkaTestProducer(topic, isMultiPartition, isMultiCluster);
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

  @BeforeClass
  public static void beforeClass()
  {
    embededKafka = new KafkaOperatorTestBase();
    embededKafka.start();
  }

  @AfterClass
  public static void afterClass()
  {
    embededKafka.stop();
  }
}

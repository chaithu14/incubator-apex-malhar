package com.datatorrent.contrib.storm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datatorrent.api.Context;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import clojure.lang.Atom;

public class Helper
{
  final static String TOPOLOGY_NAME = "storm.topology.name";
  private static int tid;

  static synchronized TopologyContext createTopologyContext(
    final Context.OperatorContext context, final IComponent spoutOrBolt,
    final String operatorName, StormTopology stormTopology, final Map stormConfig) {

    //final Partitioner<? extends Operator> dop = context.getValue(Context.OperatorContext.PARTITIONER);

    final Map<Integer, String> taskToComponents = new HashMap<Integer, String>();
    final Map<String, List<Integer>> componentToSortedTasks = new HashMap<String, List<Integer>>();
    final Map<String, Map<String, Fields>> componentToStreamToFields = new HashMap<String, Map<String, Fields>>();
    String stormId = (String) stormConfig.get(TOPOLOGY_NAME);
    String codeDir = null; // not supported
    String pidDir = null; // not supported
    Integer taskId = null;
    Integer workerPort = null; // not supported
    List<Integer> workerTasks = new ArrayList<Integer>();
    final Map<String, Object> defaultResources = new HashMap<String, Object>();
    final Map<String, Object> userResources = new HashMap<String, Object>();
    final Map<String, Object> executorData = new HashMap<String, Object>();
    final Map registeredMetrics = new HashMap();
    Atom openOrPrepareWasCalled = null;

    if (stormTopology == null) {
      // embedded mode
      ComponentCommon common = new ComponentCommon();
      common.set_parallelism_hint(1);

      HashMap<String, SpoutSpec> spouts = new HashMap<String, SpoutSpec>();
      HashMap<String, Bolt> bolts = new HashMap<String, Bolt>();
      if (spoutOrBolt instanceof IRichSpout) {
        spouts.put(operatorName, new SpoutSpec(null, common));
      } else {
        assert (spoutOrBolt instanceof IRichBolt);
        bolts.put(operatorName, new Bolt(null, common));
      }
      stormTopology = new StormTopology(spouts, bolts, new HashMap<String, StateSpoutSpec>());

      taskId = context.getId();

      List<Integer> sortedTasks = new ArrayList<Integer>(1);
      for (int i = 0; i < 1; ++i) {
        taskToComponents.put(taskId + i, operatorName);
        sortedTasks.add(i);
      }
      componentToSortedTasks.put(operatorName, sortedTasks);

      SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();
      spoutOrBolt.declareOutputFields(declarer);
      componentToStreamToFields.put(operatorName, declarer.outputStreams);
    } else {
      Map<String, SpoutSpec> spouts = stormTopology.get_spouts();
      Map<String, Bolt> bolts = stormTopology.get_bolts();
      Map<String, StateSpoutSpec> stateSpouts = stormTopology.get_state_spouts();

      tid = 1;

      for (Map.Entry<String, SpoutSpec> spout : spouts.entrySet()) {
        Integer rc = processSingleOperator(spout.getKey(), spout.getValue().get_common(),
          operatorName, context.getId(), 1, taskToComponents,
          componentToSortedTasks, componentToStreamToFields);
        if (rc != null) {
          taskId = rc;
        }
      }
      for (Map.Entry<String, Bolt> bolt : bolts.entrySet()) {
        Integer rc = processSingleOperator(bolt.getKey(), bolt.getValue().get_common(),
          operatorName, context.getId(), 1, taskToComponents,
          componentToSortedTasks, componentToStreamToFields);
        if (rc != null) {
          taskId = rc;
        }
      }
      for (Map.Entry<String, StateSpoutSpec> stateSpout : stateSpouts.entrySet()) {
        Integer rc = processSingleOperator(stateSpout.getKey(), stateSpout
            .getValue().get_common(), operatorName, context.getId(),
          1, taskToComponents, componentToSortedTasks, componentToStreamToFields);
        if (rc != null) {
          taskId = rc;
        }
      }

      assert(taskId != null);
    }

    if (!stormConfig.containsKey(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)) {
      stormConfig.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 30); // Storm default value
    }

    return new TopologyContext(stormTopology, stormConfig, taskToComponents,
      componentToSortedTasks, componentToStreamToFields, stormId, codeDir, pidDir,
      taskId, workerPort, workerTasks, defaultResources, userResources, executorData,
      registeredMetrics, openOrPrepareWasCalled);
  }

  private static Integer processSingleOperator(final String componentId,
                                               final ComponentCommon common, final String operatorName, final int index,
                                               final int dop, final Map<Integer, String> taskToComponents,
                                               final Map<String, List<Integer>> componentToSortedTasks,
                                               final Map<String, Map<String, Fields>> componentToStreamToFields) {
    final int parallelism_hint = common.get_parallelism_hint();
    Integer taskId = null;

    if (componentId.equals(operatorName)) {
      taskId = tid + index;
    }

    List<Integer> sortedTasks = new ArrayList<Integer>(dop);
    for (int i = 0; i < parallelism_hint; ++i) {
      taskToComponents.put(tid, componentId);
      sortedTasks.add(tid);
      ++tid;
    }
    componentToSortedTasks.put(componentId, sortedTasks);

    Map<String, Fields> outputStreams = new HashMap<String, Fields>();
    for(Map.Entry<String, StreamInfo> outStream : common.get_streams().entrySet()) {
      outputStreams.put(outStream.getKey(), new Fields(outStream.getValue().get_output_fields()));
    }
    componentToStreamToFields.put(componentId, outputStreams);

    return taskId;
  }


}

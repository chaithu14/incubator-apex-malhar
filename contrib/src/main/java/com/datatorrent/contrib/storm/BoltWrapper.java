package com.datatorrent.contrib.storm;

import java.util.HashMap;
import java.util.Map;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import backtype.storm.generated.StormTopology;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BoltWrapper implements Operator
{
  @FieldSerializer.Bind(JavaSerializer.class)
  private IRichBolt bolt;
  private Map config = new HashMap();
  private String name;
  private StormTopology stormTopology;
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort output = new DefaultOutputPort();
  public BoltWrapper()
  {

  }
  public BoltWrapper(final IRichBolt bolt, String name) throws IllegalArgumentException {
    this.bolt = bolt;
    this.name = name;
  }

  private transient BoltCollector outputCollector;
  public final transient DefaultInputPort<Values> input = new DefaultInputPort<Values>(){

    @Override
    public void process(Values tuple)
    {
      bolt.execute(new StormTuple(tuple));
    }
    
  };
  @Override
  public void beginWindow(long l)
  {

  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    this.outputCollector = new BoltCollector(output);
    OutputCollector stormCollector = new BoltOutputCollectorWrapper(outputCollector);
    //OutputCollector stormCollector = new OutputCollector(null);
    final TopologyContext topologyContext = Helper.createTopologyContext(context, this.bolt, this.name, this.stormTopology, config);
    this.bolt.prepare(config, topologyContext, stormCollector);

  }

  @Override
  public void teardown()
  {
    this.bolt.cleanup();
  }

  /*public BoltCollector getOutputCollector()
  {
    return outputCollector;
  }*/

  public String getName()
  {
    return name;
  }

  public IRichBolt getBolt()
  {
    return bolt;
  }

  public void setBolt(IRichBolt bolt)
  {
    this.bolt = bolt;
  }

  public void setName(String name)
  {
    this.name = name;
  }
}

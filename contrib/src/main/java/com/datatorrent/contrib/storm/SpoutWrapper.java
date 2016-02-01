package com.datatorrent.contrib.storm;

import java.util.HashMap;
import java.util.Map;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.IRichSpout;

public class SpoutWrapper implements InputOperator, Operator.ActivationListener
{
  public final transient DefaultOutputPort output = new DefaultOutputPort();
  @FieldSerializer.Bind(JavaSerializer.class)
  private IRichSpout spout;
  private String name;
  private transient SpoutCollector collector;
  private StormTopology stormTopology;

  public SpoutWrapper()
  {

  }
  public SpoutWrapper(IRichSpout spout, String name)
  {
    this.spout = spout;
    this.name = name;
  }

  @Override
  public void emitTuples()
  {
    this.spout.nextTuple();
  }

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
    this.collector = new SpoutCollector(output);
    Map map = new HashMap<>();
    this.spout.open(map, Helper.createTopologyContext(context, this.spout, this.name, this.stormTopology, map),
      new SpoutOutputCollectorWrapper(this.collector));
  }

  @Override
  public void teardown()
  {
    this.spout.close();
  }

  @Override
  public void activate(Context context)
  {
    this.spout.activate();
  }

  @Override
  public void deactivate()
  {
    this.spout.deactivate();
  }

  public StormTopology getStormTopology()
  {
    return stormTopology;
  }

  public void setStormTopology(StormTopology stormTopology)
  {
    this.stormTopology = stormTopology;
  }

  public SpoutCollector getCollector()
  {
    return collector;
  }

  public IRichSpout getSpout()
  {
    return spout;
  }

  public String getName()
  {
    return name;
  }

  public void setSpout(IRichSpout spout)
  {
    this.spout = spout;
  }

  public void setName(String name)
  {
    this.name = name;
  }
}

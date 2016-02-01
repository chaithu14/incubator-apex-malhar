package com.datatorrent.demos.stormdemo.operators;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import java.util.Map;

/**
 * Base class for Spouts that read data line by line from an arbitrary source. The declared output schema has a single
 * attribute called {@code line} and should be of type {@link String}.
 */
public abstract class AbstractLineSpout implements IRichSpout {
  private static final long serialVersionUID = 8876828403487806771L;

  public final static String ATTRIBUTE_LINE = "line";

  protected SpoutOutputCollector collector;

  @SuppressWarnings("rawtypes")
  @Override
  public void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void close() {/* noting to do */}

  @Override
  public void activate() {/* noting to do */}

  @Override
  public void deactivate() {/* noting to do */}

  @Override
  public void ack(final Object msgId) {/* noting to do */}

  @Override
  public void fail(final Object msgId) {/* noting to do */}

  @Override
  public void declareOutputFields(final OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(ATTRIBUTE_LINE));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}



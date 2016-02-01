package com.datatorrent.demos.stormdemo.operators;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public abstract class AbstractBoltSink implements IRichBolt
{
  private static final long serialVersionUID = -1626323806848080430L;

  private StringBuilder lineBuilder;
  private String prefix = "";
  private final OutputFormatter formatter;

  public AbstractBoltSink(final OutputFormatter formatter) {
    this.formatter = formatter;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public final void prepare(final Map stormConf, final TopologyContext context,
                            final OutputCollector collector) {
    this.prepareSimple(stormConf, context);
    if (context.getThisComponentId() != null && context.getComponentCommon(context.getThisComponentId()).get_parallelism_hint() > 1) {
      this.prefix = context.getThisTaskId() + "> ";
    }
  }

  protected abstract void prepareSimple(final Map<?, ?> stormConf, final TopologyContext context);

  @Override
  public final void execute(final Tuple input) {
    this.lineBuilder = new StringBuilder();
    this.lineBuilder.append(this.prefix);
    this.lineBuilder.append(this.formatter.format(input));
    this.writeExternal(this.lineBuilder.toString());
  }

  protected abstract void writeExternal(final String line);

  @Override
  public void cleanup() {/* nothing to do */}

  @Override
  public final void declareOutputFields(final OutputFieldsDeclarer declarer) {/* nothing to do */}

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

}

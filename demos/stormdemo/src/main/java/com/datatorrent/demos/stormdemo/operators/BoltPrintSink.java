package com.datatorrent.demos.stormdemo.operators;

import backtype.storm.task.TopologyContext;

import java.util.Map;

/**
 * Implements a sink that prints the received data to {@code stdout}.
 */
public final class BoltPrintSink extends AbstractBoltSink {
  private static final long serialVersionUID = -6650011223001009519L;

  public BoltPrintSink(OutputFormatter formatter) {
    super(formatter);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepareSimple(final Map stormConf, final TopologyContext context) {
		/* nothing to do */
  }

  @Override
  public void writeExternal(final String line) {
    System.out.println(line);
  }

}

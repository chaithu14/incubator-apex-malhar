package com.datatorrent.demos.stormdemo.operators;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

/**
 * Implements a Spout that reads data from {@link WordCountData#WORDS}.
 */
public final class WordCountInMemorySpout extends FiniteInMemorySpout {
  private static final long serialVersionUID = 8832143302409465843L;
  private static String[] words = {"This", "is","Spout"};

  public WordCountInMemorySpout() {
    super(words);
  }

  @Override
  public void declareOutputFields(final OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sentence"));
  }
}

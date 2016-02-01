package com.datatorrent.demos.stormdemo.operators;

import backtype.storm.tuple.Values;

public class InMemorySpout<T> extends AbstractLineSpout {
  private static final long serialVersionUID = -4008858647468647019L;

  protected T[] source;
  protected int counter = 0;

  public InMemorySpout(T[] source) {
    this.source = source;
  }

  @Override
  public void nextTuple() {
    if (this.counter < source.length) {
      this.collector.emit(new Values(source[this.counter++]));
    }
  }

}

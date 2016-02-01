package com.datatorrent.demos.stormdemo.operators;

import backtype.storm.topology.IRichSpout;

public class FiniteInMemorySpout extends InMemorySpout<String> implements IRichSpout
{
  private static final long serialVersionUID = -4008858647468647019L;

  public FiniteInMemorySpout(String[] source) {
    super(source);
  }

  public boolean reachedEnd() {
    return counter >= source.length;
  }

}

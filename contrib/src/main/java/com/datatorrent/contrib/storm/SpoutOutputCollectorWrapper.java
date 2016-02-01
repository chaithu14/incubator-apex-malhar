package com.datatorrent.contrib.storm;

import java.io.Serializable;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;

public class SpoutOutputCollectorWrapper extends SpoutOutputCollector implements Serializable
{
  public SpoutOutputCollectorWrapper(ISpoutOutputCollector delegate)
  {
    super(delegate);
  }

  public SpoutOutputCollectorWrapper()
  {
    super(null);
  }
}

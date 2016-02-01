package com.datatorrent.contrib.storm;

import java.util.List;

import com.datatorrent.api.DefaultOutputPort;

import backtype.storm.spout.ISpoutOutputCollector;

public class SpoutCollector implements ISpoutOutputCollector
{

  public final transient DefaultOutputPort output;

  public SpoutCollector()
  {
    output = null;
  }
  public SpoutCollector(DefaultOutputPort out)
  {
    this.output = out;
  }
  //public final transient DefaultOutputPort output = new DefaultOutputPort();

  @Override
  public List<Integer> emit(String s, List<Object> list, Object o)
  {
    output.emit(list);
    return null;
  }

  @Override
  public void emitDirect(int i, String s, List<Object> list, Object o)
  {
    throw new UnsupportedOperationException("Direct emit is not supported by Apex");
  }

  @Override
  public void reportError(Throwable throwable)
  {

  }
}

package com.datatorrent.contrib.storm;

import java.util.Collection;
import java.util.List;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import backtype.storm.task.IOutputCollector;
import backtype.storm.tuple.Tuple;

public class BoltCollector implements IOutputCollector
{
  @OutputPortFieldAnnotation(optional = true)
  public transient final DefaultOutputPort out;

  
  public BoltCollector(){
    this.out=new DefaultOutputPort<>();
  }
  @Override
  public List<Integer> emit(final String streamId, final Collection<Tuple> anchors, final List<Object> tuple)
  {
    out.emit(tuple);
    return null;
  }

  @Override
  public void emitDirect(int i, String s, Collection<Tuple> collection, List<Object> list)
  {
    throw new UnsupportedOperationException("Direct emit is not supported by Apex");
  }

  @Override
  public void ack(Tuple tuple)
  {

  }

  @Override
  public void fail(Tuple tuple)
  {

  }

  @Override
  public void reportError(Throwable throwable)
  {

  }
}

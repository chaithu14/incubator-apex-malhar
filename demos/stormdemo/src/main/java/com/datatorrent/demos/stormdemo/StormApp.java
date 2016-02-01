package com.datatorrent.demos.stormdemo;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.storm.BoltWrapper;
import com.datatorrent.contrib.storm.SpoutWrapper;
import com.datatorrent.demos.stormdemo.operators.BoltPrintSink;
import com.datatorrent.demos.stormdemo.operators.OutputFormatter;
import com.datatorrent.demos.stormdemo.operators.WordCountInMemorySpout;

@ApplicationAnnotation(name="stormApplication")
public class StormApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    SpoutWrapper wordcount = new SpoutWrapper();
    wordcount.setSpout(new WordCountInMemorySpout());
    wordcount.setName("read");
    BoltWrapper printword = new BoltWrapper();
    printword.setBolt(new BoltPrintSink(new OutputFormatter()));
    printword.setName("write");

    dag.addOperator("read", wordcount);
    dag.addOperator("write", printword);
    dag.addStream("writingStrings", wordcount.output, printword.input);
    /*final TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(spoutId, new WordCountInMemorySpout());
    // split up the lines in pairs (2-tuples) containing: (word,1)
    builder.setBolt(tokenierzerId, new BoltTokenizer(), 4).shuffleGrouping(spoutId);
    // group by the tuple field "0" and sum up tuple field "1"
    builder.setBolt(counterId, new BoltCounter(), 4).fieldsGrouping(tokenierzerId,
      new Fields(BoltTokenizer.ATTRIBUTE_WORD));
    builder.setBolt(sinkId, new BoltPrintSink(formatter), 4).shuffleGrouping(counterId);*/

  }
}

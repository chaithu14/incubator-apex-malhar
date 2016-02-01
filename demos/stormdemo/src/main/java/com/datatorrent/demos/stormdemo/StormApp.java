package com.datatorrent.demos.stormdemo;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.storm.BoltWrapper;
import com.datatorrent.contrib.storm.SpoutWrapper;
import com.datatorrent.demos.stormdemo.operators.BoltCounter;
import com.datatorrent.demos.stormdemo.operators.BoltPrintSink;
import com.datatorrent.demos.stormdemo.operators.BoltTokenizer;
import com.datatorrent.demos.stormdemo.operators.OutputFormatter;
import com.datatorrent.demos.stormdemo.operators.WordCountInMemorySpout;

import backtype.storm.generated.Bolt;

@ApplicationAnnotation(name="stormApplication")
public class StormApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    SpoutWrapper wordcount = dag.addOperator("read",new SpoutWrapper(new WordCountInMemorySpout(), "read"));
    BoltWrapper printword = dag.addOperator("write", new BoltWrapper(new BoltPrintSink(new OutputFormatter()),"write"));
    BoltWrapper tokenizer = dag.addOperator("tokenizing", new BoltWrapper(new BoltTokenizer(), "tokenizing"));
    BoltWrapper counter = dag.addOperator("count", new BoltWrapper(new BoltCounter(), "count"));

    dag.setAttribute(counter, Context.OperatorContext.STATELESS, true);

    dag.addStream("StringToTokens", wordcount.output, tokenizer.input);
    dag.addStream("CountTokens", tokenizer.output, counter.input);
    dag.addStream("PrintCount", counter.output, printword.input);
    //dag.addStream("writingStrings", wordcount.output, printword.input);
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

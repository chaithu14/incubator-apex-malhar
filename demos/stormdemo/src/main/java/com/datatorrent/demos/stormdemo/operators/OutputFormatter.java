package com.datatorrent.demos.stormdemo.operators;

import backtype.storm.tuple.Tuple;

import java.io.Serializable;

public class OutputFormatter implements Serializable {

  /**
   * Converts a Storm {@link Tuple} to a string. This method is used for formatting the output tuples before writing
   * them out to a file or to the console.
   *
   * @param input
   *            The tuple to be formatted
   * @return The string result of the formatting
   */
  public String format(Tuple input)
  {
    final StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("(");
    for (final Object attribute : input.getValues()) {
      stringBuilder.append(attribute);
      stringBuilder.append(",");
    }
    stringBuilder.replace(stringBuilder.length() - 1, stringBuilder.length(), ")");
    return stringBuilder.toString();
  }


}

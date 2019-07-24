package org.apache.pinot.tools.tuner.query.src.parser;

import javax.annotation.Nullable;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;


/**
 * Parser interface for a query line
 */
public interface QueryParser {
  /**
   * parse the the complete log line to a parsed obj
   * @param line the complete log line to be parsed, QuerySrc should put broken lines together
   * @return the parsed log line obj
   */
  @Nullable
  AbstractQueryStats parse(String line);
}


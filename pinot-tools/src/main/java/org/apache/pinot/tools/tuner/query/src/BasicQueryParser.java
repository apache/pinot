package org.apache.pinot.tools.tuner.query.src;

import javax.annotation.Nullable;


/**
 * Parser interface for (Log, Kafka event) parsers
 */
public interface BasicQueryParser {
  /**
   * parse the the complete log line to a parsed obj
   * @param line the complete log line to be parsed, QuerySrc should put broken lines together
   * @return the parsed log line obj
   */
  @Nullable
  BasicQueryStats parse(String line);
}


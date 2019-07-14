package org.apache.pinot.tools.tuner.query.src;

import javax.annotation.Nullable;


/*
 * Parser interface for (Log, Kafka event) parsers
 */
public interface BasicQueryParser {
  @Nullable
  BasicQueryStats parse(String line);
}


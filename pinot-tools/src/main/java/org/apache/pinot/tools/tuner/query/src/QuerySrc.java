package org.apache.pinot.tools.tuner.query.src;

import java.util.NoSuchElementException;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;


/**
 * Iterator interface for query text & related stats
 */
public interface QuerySrc {
  /**
   *
   * @return If the input has next stats obj
   */
  boolean hasNext();

  /**
   *
   * @return The nex obj parsed from input
   * @throws NoSuchElementException
   */
  AbstractQueryStats next()
      throws NoSuchElementException;
}

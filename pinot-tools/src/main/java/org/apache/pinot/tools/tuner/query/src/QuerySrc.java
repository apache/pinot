package org.apache.pinot.tools.tuner.query.src;

import java.util.NoSuchElementException;


/**
 * Iterator interface for query text & related stats
 */
public interface QuerySrc {
  /**
   *
   * @return if the input has next stats obj
   */
  boolean hasNext();

  /**
   *
   * @return the nex obj parsed from input
   * @throws NoSuchElementException
   */
  BasicQueryStats next()
      throws NoSuchElementException;
}

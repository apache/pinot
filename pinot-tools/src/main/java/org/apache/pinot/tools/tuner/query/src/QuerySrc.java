package org.apache.pinot.tools.tuner.query.src;

import java.util.NoSuchElementException;


/*
 * Iterator interface for query text & related stats
 */
public interface QuerySrc {
  boolean hasNext();

  BasicQueryStats next()
      throws NoSuchElementException;
}

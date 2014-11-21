package com.linkedin.pinot.core.chunk.creator;

import java.io.IOException;

/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 21, 2014
 */

public interface InvertedIndexCreator {
  public void add(int dictionaryId, int docId);

  public void add(Object dictionaryIds, int docIds);

  public long totalTimeTakeSoFar();

  public void seal() throws IOException;
}

package com.linkedin.pinot.core.segment.creator;

import java.io.IOException;

/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 21, 2014
 *
 * Implementation of this class is used to create inverted Indexes
 * Currently two implementations are
 * p4Delta and
 * RoaringBitmap
 */

public interface InvertedIndexCreator {
  public void add(int dictionaryId, int docId);

  public void add(Object dictionaryIds, int docIds);

  public long totalTimeTakeSoFar();

  public void seal() throws IOException;
}

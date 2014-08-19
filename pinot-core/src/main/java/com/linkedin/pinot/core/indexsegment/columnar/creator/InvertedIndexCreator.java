package com.linkedin.pinot.core.indexsegment.columnar.creator;

import java.io.IOException;

public interface InvertedIndexCreator {

  public void add(int dictionaryId, int docId);
  public long totalTimeTakeSoFar ();
  public void seal() throws IOException;
}

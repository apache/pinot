package com.linkedin.pinot.core.data.readers.sort;

import java.io.IOException;
import java.util.List;


interface SegmentSorter {

  /**
   * Return the sorted docIds
   * @param sortOrder
   * @return
   */
  int[] getSortedDocIds(final List<String> sortOrder) throws IOException;

}

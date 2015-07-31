/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.startree;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;

public interface StarTreeTable {
  /**
   * Appends a raw dimension combination to the underlying table
   *
   * <p>
   *   If this is a view of a table, the rows will be inserted after the view, within the
   *   containing table.
   * </p>
   */
  void append(StarTreeTableRow row);

  /** Return the number of rows in the table */
  int size();

  /** Returns an iterator over unique dimension combinations, with excludedDimensions aliased to ALL (and metrics aggregated) */
  Iterator<StarTreeTableRow> getUniqueCombinations(List<Integer> excludedDimensions);

  /** Return an iterator over all combinations in the table */
  Iterator<StarTreeTableRow> getAllCombinations();

  /** Sorts the table in the order provided by sortDimensions within a view */
  void sort(List<Integer> sortDimensions);

  /** Returns unique values for a dimension, and their unique / total record counts. */
  StarTreeTableGroupByStats groupBy(Integer dimension);

  /** Returns a projection of the table, which can be further sorted, etc. */
  StarTreeTable view(Integer startDocumentId, Integer documentCount);

  /** Prints table contents to STDOUT (for debugging) */
  void printTable(PrintStream printStream);

  /** Closes table and releases any external resources */
  void close() throws IOException;
}

/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.minion.segment;

import com.linkedin.pinot.core.data.GenericRow;
import java.util.List;


/**
 * Interface for record aggregator
 */
public interface RecordAggregator {

  /**
   * Aggregate grouped records into a single row. Output row needs to contain all the columns (dimension, metric, time).
   *
   * @param rows a group of rows that shares the same values for the group-by columns
   * @return an aggregated row
   */
  GenericRow aggregateRecords(List<GenericRow> rows);
}

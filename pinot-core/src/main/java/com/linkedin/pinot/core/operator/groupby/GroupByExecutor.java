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
package com.linkedin.pinot.core.operator.groupby;

import java.io.Serializable;
import java.util.List;
import java.util.Map;


/**
 * Interface class for executing the actual group-by operation.
 */
public interface GroupByExecutor {

  /**
   * Initializations that need to be performed before process can be called.
   */
  void init();

  /**
   * Performs the actual group-by aggregation on the given docId's.
   *
   * @param docIdSet
   * @param startIndex
   * @param length
   */
  void process(int[] docIdSet, int startIndex, int length);

  /**
   * Post processing (if any) to be done after all docIdSets have been processed, and
   * before getResult can be called.
   */
  void finish();

  /**
   * Returns the result of group-by aggregation, ensures that 'finish' has been
   * called before calling getResult().
   *
   * @return
   */
  List<Map<String, Serializable>> getResult();
}

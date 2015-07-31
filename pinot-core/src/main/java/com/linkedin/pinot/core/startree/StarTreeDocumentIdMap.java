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

import java.util.List;
import java.util.Set;

public interface StarTreeDocumentIdMap {
  /**
   * Records a document ID for a dimension combination.
   */
  void recordDocumentId(List<Integer> dimensions, int documentId);

  /**
   * Returns the possible document IDs that a dimension combination maps to.
   *
   * <p>
   *   These are adjusted document IDs, such that there may be repeated IDs in the raw and aggregate segments.
   * </p>
   */
  Integer getNextDocumentId(List<Integer> dimensions);
}

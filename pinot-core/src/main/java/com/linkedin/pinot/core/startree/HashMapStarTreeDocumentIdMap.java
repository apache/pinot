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

import java.util.*;

public class HashMapStarTreeDocumentIdMap implements StarTreeDocumentIdMap {
  private final Map<List<Integer>, Set<Integer>> documentIds;
  private final Set<Integer> usedRawDocumentIds;
  private final Set<Integer> usedAggDocumentIds;

  public HashMapStarTreeDocumentIdMap() {
    this.documentIds = new HashMap<List<Integer>, Set<Integer>>();
    this.usedRawDocumentIds = new HashSet<Integer>();
    this.usedAggDocumentIds = new HashSet<Integer>();
  }

  @Override
  public void recordDocumentId(List<Integer> dimensions, int documentId) {
    Set<Integer> ids = documentIds.get(dimensions);
    if (ids == null) {
      ids = new HashSet<Integer>(1); // just assume all are unique, will resize
      documentIds.put(dimensions, ids);
    }
    ids.add(documentId);
  }

  @Override
  public Integer getNextDocumentId(List<Integer> dimensions) {
    Set<Integer> used;
    if (dimensions.contains(StarTreeIndexNode.all())) {
      used = usedAggDocumentIds;
    } else {
      used = usedRawDocumentIds;
    }

    Set<Integer> possible = documentIds.get(dimensions);
    if (possible == null) {
      return null;
    }

    Integer next = null;

    for (Integer id : possible) {
      if (!used.contains(id)) {
        used.add(id);
        next = id;
        break;
      }
    }

    return next;
  }

  @Override
  public String toString() {
    return documentIds.toString();
  }
}

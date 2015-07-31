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

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.*;

public class LinkedListStarTreeTable implements StarTreeTable {
  private final List<StarTreeTableRow> list;

  public LinkedListStarTreeTable() {
    this(null);
  }

  private LinkedListStarTreeTable(List<StarTreeTableRow> list) {
    if (list == null) {
      this.list = new LinkedList<StarTreeTableRow>();
    } else {
      this.list = list;
    }
  }

  @Override
  public void append(StarTreeTableRow row) {
    list.add(row);
  }

  @Override
  public int size() {
    return list.size();
  }

  @Override
  public Iterator<StarTreeTableRow> getUniqueCombinations(List<Integer> excludedDimensions) {
    Map<List<Integer>, List<Number>> uniqueCombinations = new HashMap<List<Integer>, List<Number>>();

    for (StarTreeTableRow combination : list) {
      List<Integer> aliased = new ArrayList<Integer>(combination.getDimensions());
      // Alias all excludedDimensions to all
      for (int i = 0; i < aliased.size(); i++) {
        if (excludedDimensions != null && excludedDimensions.contains(i)) {
          aliased.set(i, StarTreeIndexNode.all());
        }
      }

      // Aggregate metrics
      List<Number> aggregates = uniqueCombinations.get(aliased);
      if (aggregates == null) {
        aggregates = new ArrayList<Number>();
        for (int i = 0; i < combination.getMetrics().size(); i++) {
          aggregates.add(0);
        }
        uniqueCombinations.put(aliased, aggregates);
      }
      aggregateOnto(aggregates, combination.getMetrics());
    }

    final Iterator<Map.Entry<List<Integer>, List<Number>>> itr = uniqueCombinations.entrySet().iterator();
    return new Iterator<StarTreeTableRow>() {
      @Override
      public boolean hasNext() {
        return itr.hasNext();
      }

      @Override
      public StarTreeTableRow next() {
        Map.Entry<List<Integer>, List<Number>> next = itr.next();
        return new StarTreeTableRow(next.getKey(), next.getValue());
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public Iterator<StarTreeTableRow> getAllCombinations() {
    return list.iterator();
  }

  @Override
  public void sort(final List<Integer> sortDimensions) {
    Collections.sort(list, new Comparator<StarTreeTableRow>() {
      @Override
      public int compare(StarTreeTableRow e1, StarTreeTableRow e2) {
        for (Integer sortDimension : sortDimensions) {
          Integer v1 = e1.getDimensions().get(sortDimension);
          Integer v2 = e2.getDimensions().get(sortDimension);
          if (!v1.equals(v2)) {
            return v1.compareTo(v2);
          }
        }
        return 0;
      }
    });
  }

  @Override
  public StarTreeTableGroupByStats groupBy(Integer dimension) {
    Map<Integer, Set<List<Integer>>> uniqueCombinations = new HashMap<Integer, Set<List<Integer>>>();
    Map<Integer, Integer> rawCounts = new HashMap<Integer, Integer>();
    Map<Integer, Integer> minRecordIds = new HashMap<Integer, Integer>();

    int currentRecordId = 0;
    for (StarTreeTableRow row : list) {
      Integer value = row.getDimensions().get(dimension);

      // Unique
      Set<List<Integer>> uniqueSet = uniqueCombinations.get(value);
      if (uniqueSet == null) {
        uniqueSet = new HashSet<List<Integer>>();
        uniqueCombinations.put(value, uniqueSet);
      }
      uniqueSet.add(row.getDimensions());

      // Raw
      Integer count = rawCounts.get(value);
      if (count == null) {
        count = 0;
      }
      rawCounts.put(value, count + 1);

      // Record ID
      Integer existingRecordId = minRecordIds.get(value);
      if (existingRecordId == null || currentRecordId < existingRecordId) {
        minRecordIds.put(value, currentRecordId);
      }
      currentRecordId++;
    }

    StarTreeTableGroupByStats result = new StarTreeTableGroupByStats();
    for (Map.Entry<Integer, Set<List<Integer>>> entry : uniqueCombinations.entrySet()) {
      Integer value = entry.getKey();
      Integer uniqueCount = entry.getValue().size();
      Integer rawCount = rawCounts.get(value);
      Integer minRecordId = minRecordIds.get(value);
      result.setValue(value, rawCount, uniqueCount, minRecordId);
    }

    return result;
  }

  @Override
  public StarTreeTable view(Integer startDocumentId, Integer documentCount) {
    return new LinkedListStarTreeTable(list.subList(startDocumentId, startDocumentId + documentCount));
  }

  @Override
  public void printTable(PrintStream printStream) {
    for (int i = 0; i < list.size(); i++) {
      printStream.println(i + ": " + list.get(i));
    }
  }

  @Override
  public void close() {
    // NOP
  }

  private void aggregateOnto(List<Number> base, List<Number> increments) {
    for (int i = 0; i < increments.size(); i++) {
      Number increment = increments.get(i);
      if (increment instanceof Integer) {
        base.set(i, base.get(i).intValue() + increment.intValue());
      } else if (increment instanceof Long) {
        base.set(i, base.get(i).longValue() + increment.longValue());
      } else if (increment instanceof Float) {
        base.set(i, base.get(i).floatValue() + increment.floatValue());
      } else if (increment instanceof Double) {
        base.set(i, base.get(i).doubleValue() + increment.doubleValue());
      } else {
        throw new IllegalStateException("Unsupported metric type: " + increment.getClass());
      }
    }
  }
}


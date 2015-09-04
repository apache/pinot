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

import com.linkedin.pinot.common.data.FieldSpec;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.*;

public class LinkedListStarTreeTable implements StarTreeTable {
  private final List<ByteBuffer> list;

  protected final List<FieldSpec.DataType> dimensionTypes;
  protected final List<FieldSpec.DataType> metricTypes;
  protected final int rowSize;

  public LinkedListStarTreeTable(List<FieldSpec.DataType> dimensionTypes, List<FieldSpec.DataType> metricTypes) {
    this(null, dimensionTypes, metricTypes);
  }

  private LinkedListStarTreeTable(List<ByteBuffer> list,
                                  List<FieldSpec.DataType> dimensionTypes,
                                  List<FieldSpec.DataType> metricTypes) {
    if (list == null) {
      this.list = new LinkedList<ByteBuffer>();
    } else {
      this.list = list;
    }

    this.dimensionTypes = dimensionTypes;
    this.metricTypes = metricTypes;
    this.rowSize = getRowSize();
  }

  @Override
  public void append(StarTreeTableRow row) {
    ByteBuffer buffer = toByteBuffer(row);
    list.add(buffer);
  }

  @Override
  public int size() {
    return list.size();
  }

  @Override
  public Iterator<StarTreeTableRow> getUniqueCombinations(final List<Integer> excludedDimensions) {
    // Get the included dimensions
    final List<Integer> includedDimensions = new ArrayList<>();
    for (int i = 0; i < dimensionTypes.size(); i++) {
      if (excludedDimensions == null || !excludedDimensions.contains(i)) {
        includedDimensions.add(i);
      }
    }

    final Comparator<StarTreeTableRow> includedComparator = new Comparator<StarTreeTableRow>() {
      @Override
      public int compare(StarTreeTableRow o1, StarTreeTableRow o2) {
        for (Integer dimension : includedDimensions) {
          Integer v1 = o1.getDimensions().get(dimension);
          Integer v2 = o2.getDimensions().get(dimension);
          if (!v1.equals(v2)) {
            return v1 - v2;
          }
        }
        return 0;
      }
    };

    // Sort a copy of the list on the included dimensions
    List<ByteBuffer> listCopy = new LinkedList<>(list);
    Collections.sort(listCopy, new Comparator<ByteBuffer>() {
      @Override
      public int compare(ByteBuffer o1, ByteBuffer o2) {
        for (Integer dimension : includedDimensions) {
          o1.position(dimension * Integer.SIZE / 8);
          o2.position(dimension * Integer.SIZE / 8);
          Integer v1 = o1.getInt();
          Integer v2 = o2.getInt();
          if (!v1.equals(v2)) {
            return v1 - v2;
          }
        }
        return 0;
      }
    });

    final Iterator<ByteBuffer> itr = listCopy.iterator();

    // Iterator
    return new Iterator<StarTreeTableRow>() {
      private StarTreeTableRow nextToReturn = new StarTreeTableRow(dimensionTypes.size(), metricTypes.size());
      private StarTreeTableRow nextFromItr = new StarTreeTableRow(dimensionTypes.size(), metricTypes.size());
      private boolean shouldReset = true;
      private boolean hasReturnedCurrent = true;
      private boolean hasStarted = false;

      @Override
      public boolean hasNext() {
        if (itr.hasNext() && hasReturnedCurrent) {
          // Initialize nextToReturn with this as base
          if (shouldReset) {
            if (!hasStarted) {
              ByteBuffer buffer = itr.next();
              fromByteBuffer(buffer, nextFromItr);
              hasStarted = true;
            }
            copyStarTreeTableRow(nextFromItr, nextToReturn, excludedDimensions);
            shouldReset = false;
          }

          // While there are more rows in table and the included dimensions match, consume and aggregate
          while (itr.hasNext()) {
            ByteBuffer buffer = itr.next();
            fromByteBuffer(buffer, nextFromItr);

            if (includedComparator.compare(nextFromItr, nextToReturn) == 0) {
              aggregateStarTreeTableRow(nextFromItr, nextToReturn);
            } else {
              // Here, the next time hasNext is called we will use the contents of nextFromItr
              shouldReset = true;
              break;
            }
          }

          hasReturnedCurrent = false;
        }

        return itr.hasNext() || !hasReturnedCurrent;
      }

      @Override
      public StarTreeTableRow next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        hasReturnedCurrent = true;
        return nextToReturn;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public Iterator<StarTreeTableRow> getAllCombinations() {
    final StarTreeTableRow reuse = new StarTreeTableRow(dimensionTypes.size(), metricTypes.size());
    final Iterator<ByteBuffer> itr = list.iterator();
    return new Iterator<StarTreeTableRow>() {
      @Override
      public boolean hasNext() {
        return itr.hasNext();
      }

      @Override
      public StarTreeTableRow next() {
        ByteBuffer buffer = itr.next();
        fromByteBuffer(buffer, reuse);
        return reuse;
      }
    };
  }

  @Override
  public void sort(final List<Integer> sortDimensions) {
    if (sortDimensions != null) {
      final StarTreeTableRow fstRow = new StarTreeTableRow(dimensionTypes.size(), metricTypes.size());
      final StarTreeTableRow secRow = new StarTreeTableRow(dimensionTypes.size(), metricTypes.size());
      Collections.sort(list, new Comparator<ByteBuffer>() {
        @Override
        public int compare(ByteBuffer o1, ByteBuffer o2) {
          for (Integer sortDimension : sortDimensions) {
            o1.position(sortDimension * Integer.SIZE / 8);
            Integer v1 = o1.getInt();
            o2.position(sortDimension * Integer.SIZE / 8);
            Integer v2 = o2.getInt();
            if (!v1.equals(v2)) {
              return v1.compareTo(v2);
            }
          }
          return 0;
        }
      });
    }
  }

  @Override
  public StarTreeTableGroupByStats groupBy(Integer dimension) {
    Map<Integer, Set<List<Integer>>> uniqueCombinations = new HashMap<Integer, Set<List<Integer>>>();
    Map<Integer, Integer> rawCounts = new HashMap<Integer, Integer>();
    Map<Integer, Integer> minRecordIds = new HashMap<Integer, Integer>();

    int currentRecordId = 0;
    StarTreeTableRow row = new StarTreeTableRow(dimensionTypes.size(), metricTypes.size());
    for (ByteBuffer buffer : list) {
      fromByteBuffer(buffer, row);
      Integer value = row.getDimensions().get(dimension);

      // Unique
      Set<List<Integer>> uniqueSet = uniqueCombinations.get(value);
      if (uniqueSet == null) {
        uniqueSet = new HashSet<List<Integer>>();
        uniqueCombinations.put(value, uniqueSet);
      }
      if (!uniqueSet.contains(row.getDimensions())) {
        List<Integer> copy = new ArrayList<>(row.getDimensions());
        uniqueSet.add(copy);
      }

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
    return new LinkedListStarTreeTable(list.subList(startDocumentId, startDocumentId + documentCount), dimensionTypes, metricTypes);
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

  private int getRowSize() {
    int rowSize = 0;

    // Always int
    for (int i = 0; i < dimensionTypes.size(); i++) {
      rowSize += Integer.SIZE / 8;
    }

    for (FieldSpec.DataType dataType : metricTypes) {
      switch (dataType) {
        case SHORT:
          rowSize += Short.SIZE / 8;
          break;
        case INT:
          rowSize += Integer.SIZE / 8;
          break;
        case LONG:
          rowSize += Long.SIZE / 8;
          break;
        case FLOAT:
          rowSize += Float.SIZE / 8;
          break;
        case DOUBLE:
          rowSize += Double.SIZE / 8;
          break;
        default:
          throw new IllegalArgumentException("Unsupported metric type " + dataType);
      }
    }

    return rowSize;
  }

  protected ByteBuffer toByteBuffer(StarTreeTableRow row) {
    ByteBuffer buffer = ByteBuffer.allocate(rowSize); // TODO: Direct and Heap

    for (int i = 0; i < dimensionTypes.size(); i++) {
      buffer.putInt(row.getDimensions().get(i));
    }

    for (int i = 0; i < metricTypes.size(); i++) {
      switch (metricTypes.get(i)) {
        case SHORT:
          buffer.putShort(row.getMetrics().get(i).shortValue());
          break;
        case INT:
          buffer.putInt(row.getMetrics().get(i).intValue());
          break;
        case LONG:
          buffer.putLong(row.getMetrics().get(i).longValue());
          break;
        case FLOAT:
          buffer.putFloat(row.getMetrics().get(i).floatValue());
          break;
        case DOUBLE:
          buffer.putDouble(row.getMetrics().get(i).doubleValue());
          break;
        default:
          throw new IllegalArgumentException("Unsupported metric type " + metricTypes.get(i));
      }
    }

    return buffer;
  }

  private void fromByteBuffer(ByteBuffer buffer, StarTreeTableRow row) {
    buffer.rewind();

    for (int i = 0; i < dimensionTypes.size(); i++) {
      row.getDimensions().set(i, buffer.getInt());
    }

    for (int i = 0; i < metricTypes.size(); i++) {
      switch (metricTypes.get(i)) {
        case SHORT:
          row.setMetric(i, buffer.getShort());
          break;
        case INT:
          row.setMetric(i, buffer.getInt());
          break;
        case LONG:
          row.setMetric(i, buffer.getLong());
          break;
        case FLOAT:
          row.setMetric(i, buffer.getFloat());
          break;
        case DOUBLE:
          row.setMetric(i, buffer.getDouble());
          break;
        default:
          throw new IllegalArgumentException("Unsupported metric type " + metricTypes.get(i));
      }
    }
  }

  private void copyStarTreeTableRow(StarTreeTableRow src, StarTreeTableRow dst, List<Integer> excludedDimensions) {
    for (int i = 0; i < dimensionTypes.size(); i++) {
      if (excludedDimensions != null && excludedDimensions.contains(i)) {
        dst.setDimension(i, StarTreeIndexNode.all());
      } else {
        dst.setDimension(i, src.getDimensions().get(i));
      }
    }

    for (int i = 0; i < metricTypes.size(); i++) {
      dst.setMetric(i, src.getMetrics().get(i));
    }
  }

  private void aggregateStarTreeTableRow(StarTreeTableRow src, StarTreeTableRow dst) {
    for (int i = 0; i < metricTypes.size(); i++) {
      switch (metricTypes.get(i)) {
        case SHORT:
          dst.setMetric(i, dst.getMetrics().get(i).shortValue() + src.getMetrics().get(i).shortValue());
          break;
        case INT:
          dst.setMetric(i, dst.getMetrics().get(i).intValue() + src.getMetrics().get(i).intValue());
          break;
        case LONG:
          dst.setMetric(i, dst.getMetrics().get(i).longValue() + src.getMetrics().get(i).longValue());
          break;
        case FLOAT:
          dst.setMetric(i, dst.getMetrics().get(i).floatValue() + src.getMetrics().get(i).floatValue());
          break;
        case DOUBLE:
          dst.setMetric(i, dst.getMetrics().get(i).doubleValue() + src.getMetrics().get(i).doubleValue());
          break;
        default:
          throw new IllegalArgumentException("Unsupported metric type " + metricTypes.get(i));
      }
    }
  }
}


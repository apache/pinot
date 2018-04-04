package com.linkedin.pinot.core.data.readers.sort;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.PinotSegmentColumnReader;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntComparator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class OnHeapPinotSegmentSorter implements SegmentSorter {

  private int _numDocs;
  private Schema _schema;
  private Map<String, PinotSegmentColumnReader> _columnReaderMap;
  private int[][] _dictionaryMap;
  private int[] _sortOrder;
  private List<String> _dimensionNames;
  int _numDimensions;


  public OnHeapPinotSegmentSorter(int numDocs, Schema schema, Map<String, PinotSegmentColumnReader> columnReaderMap) {
    _numDocs = numDocs;
    _schema = schema;
    _columnReaderMap = columnReaderMap;
    _dimensionNames = new ArrayList<>();
    // Dimension fields
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      // Count all fields that are not metrics as dimensions
      if (fieldSpec.getFieldType() != FieldSpec.FieldType.METRIC) {
        String dimensionName = fieldSpec.getName();
        _numDimensions++;
        _dimensionNames.add(dimensionName);
      }
    }
  }

  @Override
  public int[] getSortedDocIds(List<String> sortOrder) throws IOException {

    _sortOrder = new int[_dimensionNames.size()];
    Set<Integer> covered = new HashSet<>();
    int index = 0;
    for (String dimension : sortOrder) {
      int dimensionId = _dimensionNames.indexOf(dimension);
      if (dimensionId != -1) {
        _sortOrder[index++] = dimensionId;
        covered.add(dimensionId);
      }
    }

    for(int i = 0; i < _dimensionNames.size(); i++) {
      if (!covered.contains(i)) {
        _sortOrder[index++] = i;
      }
    }

    System.out.println("Sort order: "  + java.util.Arrays.toString(_sortOrder));
    _dictionaryMap = new int[_dimensionNames.size()][_numDocs];
    for (int idx : _sortOrder) {
      PinotSegmentColumnReader reader = _columnReaderMap.get(_dimensionNames.get(idx));
      for (int i = 0; i < _numDocs; i++) {
        _dictionaryMap[idx][i] = reader.getDictionaryId(i);
      }
    }


    final int[] sortedDocIds = new int[_numDocs];
    for (int i = 0; i < _numDocs; i++) {
      sortedDocIds[i] = i;
    }

    IntComparator comparator = new IntComparator() {
      @Override
      public int compare(int i1, int i2) {
        int docId1 = sortedDocIds[i1];
        int docId2 = sortedDocIds[i2];

        int compare = 0;
        for (int index : _sortOrder) {
          int[] dictionary = _dictionaryMap[index];
          if (dictionary != null) {
            compare = dictionary[docId1] - dictionary[docId2];
          } else {
            throw new RuntimeException("no dictionary");
          }
          if (compare != 0) {
            return compare;
          }
        }
        return compare;
      }

      @Override
      public int compare(Integer o1, Integer o2) {
        throw new UnsupportedOperationException();
      }
    };

    Swapper swapper = new Swapper() {
      @Override
      public void swap(int i, int j) {
        int temp = sortedDocIds[i];
        sortedDocIds[i] = sortedDocIds[j];
        sortedDocIds[j] = temp;
      }
    };

    Arrays.quickSort(0, _numDocs, comparator, swapper);

    return sortedDocIds;
  }
}

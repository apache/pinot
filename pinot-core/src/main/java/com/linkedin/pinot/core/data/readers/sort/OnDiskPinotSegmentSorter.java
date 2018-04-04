package com.linkedin.pinot.core.data.readers.sort;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.PinotSegmentColumnReader;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntComparator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class OnDiskPinotSegmentSorter implements SegmentSorter {

  private int _numDocs;
  private Schema _schema;
  private Map<String, PinotSegmentColumnReader> _columnReaderMap;

  private int[] _sortOrder;
  private List<String> _dimensionNames;
  int _numDimensions;

  public OnDiskPinotSegmentSorter(int numDocs, Schema schema, Map<String, PinotSegmentColumnReader> columnReaderMap) {
    _numDocs = numDocs;
    _schema = schema;
    _columnReaderMap = columnReaderMap;
    _dimensionNames = new ArrayList<>();
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
  public int[] getSortedDocIds(final List<String> sortOrder) {

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
          FieldSpec fieldSpec = _schema.getFieldSpecFor(_dimensionNames.get(index));
          PinotSegmentColumnReader columnReader = _columnReaderMap.get(fieldSpec.getName());

          if (columnReader.hasDictionary()) {
            compare = columnReader.getDictionaryId(docId1) - columnReader.getDictionaryId(docId2);
          } else {
            throw new RuntimeException("nonono");
//            switch (fieldSpec.getDataType()) {
//              case INT:
//                compare = ((Integer) columnReader.readInt(docId1)).compareTo((Integer) columnReader.readInt(docId2));
//                break;
//              case LONG:
//                compare = ((Long) columnReader.readInt(docId1)).compareTo((Long) columnReader.readInt(docId2));
//                break;
//              case FLOAT:
//                compare = ((Float) columnReader.readInt(docId1)).compareTo((Float) columnReader.readInt(docId2));
//                break;
//              case DOUBLE:
//                compare = ((Double) columnReader.readInt(docId1)).compareTo((Double) columnReader.readInt(docId2));
//                break;
//              case STRING:
//                compare = ((String) columnReader.readInt(docId1)).compareTo((String) columnReader.readInt(docId2));
//                break;
//              default:
//                throw new IllegalStateException(
//                    "Field: " + fieldSpec.getName() + " has illegal data type: " + fieldSpec.getDataType());
//            }
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

package com.linkedin.pinot.core.startreeV2;

import java.util.List;
import java.util.ArrayList;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntComparator;

public class Experiment {

  public List<Integer>order = new ArrayList<>();
  public List<List<Object>> data = new ArrayList<List<Object>>();

  public List<Object> d1 = new ArrayList<>();
  public List<Object> d2 = new ArrayList<>();
  public List<Object> d3 = new ArrayList<>();

  public int[] sort() {

    final int[] sortedDocIds = new int[d1.size()];
    for (int i = 0; i < d1.size(); i++) {
      sortedDocIds[i] = i;
    }

    IntComparator comparator = new IntComparator() {
      @Override
      public int compare(int i1, int i2) {
        int docId1 = sortedDocIds[i1];
        int docId2 = sortedDocIds[i2];

        int compare = 0;
        for (int index : order) {
          List<Object> column = data.get(index);
          compare = (int)column.get(docId1) - (int)column.get(docId2);
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

    Arrays.quickSort(0, d1.size(), comparator, swapper);

    return sortedDocIds;
  }
}

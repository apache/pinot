package com.linkedin.pinot.core.realtime.impl.invertedIndex;

import java.util.Set;

import com.google.common.collect.Sets;

public class RealtimeInvertedIndexUtils {


  public static Integer[] and(Set<Integer> set1, Set<Integer> set2) {
    return (Integer[]) Sets.intersection(set1, set2).toArray();
  }

  public static Integer[] or(Set<Integer> set1, Set<Integer> set2) {
    return (Integer[]) Sets.union(set1, set2).toArray();
  }
}

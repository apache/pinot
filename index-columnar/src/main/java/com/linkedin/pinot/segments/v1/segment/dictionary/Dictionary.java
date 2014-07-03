package com.linkedin.pinot.segments.v1.segment.dictionary;



public abstract class Dictionary<T> {
  abstract public boolean contains(Object o);

  abstract public int indexOf(Object o);

  abstract int size();
}

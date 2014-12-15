package com.linkedin.pinot.core.query.utils;

import it.unimi.dsi.fastutil.AbstractPriorityQueue;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import com.linkedin.pinot.common.request.SelectionSort;


public class SerializableListPriorityQueue extends AbstractPriorityQueue<List<Serializable>> {

  private final List<SelectionSort> _selectionSortList;
  private final int _maxSize;

  public SerializableListPriorityQueue(List<SelectionSort> selectionSortList, int maxSize) {
    _selectionSortList = selectionSortList;
    _maxSize = maxSize;
  }

  @Override
  public void enqueue(List<Serializable> x) {
    // TODO Auto-generated method stub

  }

  @Override
  public List<Serializable> dequeue() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int size() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void clear() {
    // TODO Auto-generated method stub

  }

  @Override
  public List<Serializable> first() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Comparator<? super List<Serializable>> comparator() {
    // TODO Auto-generated method stub
    return null;
  }

}

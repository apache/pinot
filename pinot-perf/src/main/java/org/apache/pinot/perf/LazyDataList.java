/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.perf;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import org.apache.pinot.spi.data.readers.GenericRow;


public class LazyDataList implements List<GenericRow> {

  private final GenericRow _row;
  private final int _size;
  private final RowGenerator _rowGenerator;

  public interface RowGenerator {
    void generateRow(GenericRow row, int index);
  }

  public LazyDataList(int size, RowGenerator rowGenerator) {
    _row = new GenericRow();
    _size = size;
    _rowGenerator = rowGenerator;
  }

  @Override
  public int size() {
    return _size;
  }

  @Override
  public boolean isEmpty() {
    return _size == 0;
  }

  @Override
  public boolean contains(Object o) {
    return false;
  }

  @Override
  public Iterator<GenericRow> iterator() {
    return null;
  }

  @Override
  public Object[] toArray() {
    return new Object[0];
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return null;
  }

  @Override
  public boolean add(GenericRow genericRow) {
    return false;
  }

  @Override
  public boolean remove(Object o) {
    return false;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return false;
  }

  @Override
  public boolean addAll(Collection<? extends GenericRow> c) {
    return false;
  }

  @Override
  public boolean addAll(int index, Collection<? extends GenericRow> c) {
    return false;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return false;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return false;
  }

  @Override
  public void clear() {
    // do nothing
  }

  @Override
  public GenericRow get(int index) {
    generateRow(index);
    return _row;
  }

  private void generateRow(int index) {
    _rowGenerator.generateRow(_row, index);
  }

  @Override
  public GenericRow set(int index, GenericRow element) {
    return null;
  }

  @Override
  public void add(int index, GenericRow element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GenericRow remove(int index) {
    return null;
  }

  @Override
  public int indexOf(Object o) {
    return 0;
  }

  @Override
  public int lastIndexOf(Object o) {
    return 0;
  }

  @Override
  public ListIterator<GenericRow> listIterator() {
    return null;
  }

  @Override
  public ListIterator<GenericRow> listIterator(int index) {
    return null;
  }

  @Override
  public List<GenericRow> subList(int fromIndex, int toIndex) {
    return List.of();
  }
}

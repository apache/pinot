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
package org.apache.pinot.common.utils.list;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class FlatViewList<T> implements List<T> {

  private final List<T>[] _parts;

  public FlatViewList(List<T>... parts) {
    _parts = parts;
  }

  @Override
  public int size() {
    int size = 0;
    for (List<T> p: _parts) {
      size += p.size();
    }
    return size;
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public boolean contains(Object o) {
    for (List<T> p: _parts) {
      if (p.contains(o)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Iterator<T> iterator() {
    return asStream().iterator();
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <X> X[] toArray(X[] a) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean add(T t) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return false;
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(int index, Collection<? extends T> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public T get(int index) {
    if (size() < index) {
      throw new IndexOutOfBoundsException(index);
    }
    for (List<T> p : _parts) {
      if (index < p.size()) {
        return p.get(index);
      }
      index -= p.size();
    }
    throw new IllegalArgumentException();
  }

  @Override
  public T set(int index, T element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(int index, T element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T remove(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int indexOf(Object o) {
    int offset = 0;
    for (List<T> p : _parts) {
      int index = p.indexOf(o);
      if (index != -1) {
        return offset + index;
      }
      offset += p.size();
    }
    return -1;
  }

  @Override
  public int lastIndexOf(Object o) {
    for (int pi = _parts.length; pi >= 0; pi--) {
      List<T> p = _parts[pi];
      int index = p.lastIndexOf(o);
      if (index != -1) {
        int offset = 0;
        for (int pi2 = 0; pi2 < pi; pi2++) {
          offset += _parts[pi2].size();
        }
        return offset + index;
      }
    }
    return -1;
  }

  @Override
  public ListIterator<T> listIterator() {
    return asStream().collect(Collectors.toList()).listIterator();
  }

  @Override
  public ListIterator<T> listIterator(int index) {
    if (size() < index) {
      throw new IndexOutOfBoundsException(index);
    }
    return asStream().collect(Collectors.toList()).listIterator(index);
  }

  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    return asStream().collect(Collectors.toList()).subList(fromIndex, toIndex);
  }

  private Stream<T> asStream() {
    return Stream.of(_parts).flatMap(Collection::stream);
  }
}

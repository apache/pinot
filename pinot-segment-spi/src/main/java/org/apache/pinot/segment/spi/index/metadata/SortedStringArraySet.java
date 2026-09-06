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
package org.apache.pinot.segment.spi.index.metadata;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;


/// Unmodifiable [NavigableSet] view of a range of a sorted, duplicate-free `String[]`, ordered naturally.
///
/// Lookups are a binary search over the array, so the whole set costs one small object rather than a red-black-tree
/// node per element. That is the point: a server holds the column names of every loaded segment for the segment's
/// lifetime, and a wide segment has thousands of them.
///
/// The array is referenced, not copied, so the view reflects nothing the holder does afterwards *except* in-place
/// writes: [SegmentMetadataImpl] replaces its arrays when its columns change, which leaves an already-returned view
/// as the snapshot taken at the time of the call.
///
/// Immutable and thread-safe as long as the backing array is not written in place.
final class SortedStringArraySet extends AbstractSet<String> implements NavigableSet<String> {
  private final String[] _elements;
  private final int _from;
  private final int _to;

  SortedStringArraySet(String[] elements) {
    this(elements, 0, elements.length);
  }

  private SortedStringArraySet(String[] elements, int from, int to) {
    _elements = elements;
    _from = from;
    _to = to;
  }

  @Override
  public int size() {
    return _to - _from;
  }

  @Override
  public boolean isEmpty() {
    return _from == _to;
  }

  @Override
  public boolean contains(Object o) {
    return o instanceof String && search((String) o) >= 0;
  }

  /// Index of `element`, or `-(insertion point) - 1`, both absolute in [#_elements].
  private int search(String element) {
    return Arrays.binarySearch(_elements, _from, _to, element);
  }

  private int ceilingIndex(String element) {
    int index = search(element);
    return index >= 0 ? index : -index - 1;
  }

  private int higherIndex(String element) {
    int index = search(element);
    return index >= 0 ? index + 1 : -index - 1;
  }

  private int floorIndex(String element) {
    int index = search(element);
    return index >= 0 ? index : -index - 2;
  }

  private int lowerIndex(String element) {
    int index = search(element);
    return index >= 0 ? index - 1 : -index - 2;
  }

  @Nullable
  private String at(int index) {
    return index >= _from && index < _to ? _elements[index] : null;
  }

  @Nullable
  @Override
  public String ceiling(String element) {
    return at(ceilingIndex(element));
  }

  @Nullable
  @Override
  public String higher(String element) {
    return at(higherIndex(element));
  }

  @Nullable
  @Override
  public String floor(String element) {
    return at(floorIndex(element));
  }

  @Nullable
  @Override
  public String lower(String element) {
    return at(lowerIndex(element));
  }

  @Override
  public String first() {
    if (isEmpty()) {
      throw new NoSuchElementException();
    }
    return _elements[_from];
  }

  @Override
  public String last() {
    if (isEmpty()) {
      throw new NoSuchElementException();
    }
    return _elements[_to - 1];
  }

  @Nullable
  @Override
  public Comparator<? super String> comparator() {
    return null;
  }

  @Override
  public Iterator<String> iterator() {
    return new Iterator<>() {
      private int _index = _from;

      @Override
      public boolean hasNext() {
        return _index < _to;
      }

      @Override
      public String next() {
        if (_index >= _to) {
          throw new NoSuchElementException();
        }
        return _elements[_index++];
      }
    };
  }

  @Override
  public Iterator<String> descendingIterator() {
    return new Iterator<>() {
      private int _index = _to;

      @Override
      public boolean hasNext() {
        return _index > _from;
      }

      @Override
      public String next() {
        if (_index <= _from) {
          throw new NoSuchElementException();
        }
        return _elements[--_index];
      }
    };
  }

  @Override
  public NavigableSet<String> subSet(String from, boolean fromInclusive, String to, boolean toInclusive) {
    checkArgument(from.compareTo(to) <= 0, "from: %s > to: %s", from, to);
    int start = fromInclusive ? ceilingIndex(from) : higherIndex(from);
    int end = toInclusive ? higherIndex(to) : ceilingIndex(to);
    return new SortedStringArraySet(_elements, start, Math.max(start, end));
  }

  @Override
  public SortedSet<String> subSet(String from, String to) {
    return subSet(from, true, to, false);
  }

  @Override
  public NavigableSet<String> headSet(String to, boolean inclusive) {
    return new SortedStringArraySet(_elements, _from, inclusive ? higherIndex(to) : ceilingIndex(to));
  }

  @Override
  public SortedSet<String> headSet(String to) {
    return headSet(to, false);
  }

  @Override
  public NavigableSet<String> tailSet(String from, boolean inclusive) {
    return new SortedStringArraySet(_elements, inclusive ? ceilingIndex(from) : higherIndex(from), _to);
  }

  @Override
  public SortedSet<String> tailSet(String from) {
    return tailSet(from, true);
  }

  /// Unlike the range sets above this is a copy, not a view, since the backing array is ascending. Nothing on the
  /// segment paths calls it; it exists so the [NavigableSet] contract holds.
  @Override
  public NavigableSet<String> descendingSet() {
    TreeSet<String> descending = new TreeSet<>(Comparator.reverseOrder());
    descending.addAll(this);
    return Collections.unmodifiableNavigableSet(descending);
  }

  @Override
  public boolean remove(Object o) {
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
  public String pollFirst() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String pollLast() {
    throw new UnsupportedOperationException();
  }
}

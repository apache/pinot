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
package org.apache.pinot.query.planner.physical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;


public class DisjointSet<T> {

  private final Map<T, Integer> _rank;
  private final Map<T, T> _parent;

  public DisjointSet() {
    _rank = new HashMap<>();
    _parent = new HashMap<>();
  }

  public DisjointSet(Collection<T> members) {
    this();
    for (T member: members) {
      add(member);
    }
  }

  public boolean contains(T a) {
    return _rank.containsKey(a);
  }

  public List<T> getMatchingElements(Function<T, Boolean> matcher) {
    return _rank.keySet().stream().filter(matcher::apply).collect(Collectors.toList());
  }

  public void add(T a) {
    if (!_rank.containsKey(a)) {
      _rank.put(a, 1);
      _parent.put(a, a);
    }
  }

  public T findRoot(T a) {
    if (_parent.get(a) == a) {
      return a;
    }
    T newParent = findRoot(_parent.get(a));
    _parent.put(a, newParent);
    return newParent;
  }

  public int size() {
    return _rank.size();
  }

  public void merge(T a, T b) {
    T aRoot = findRoot(a);
    T bRoot = findRoot(b);
    if (aRoot == bRoot) {
      return;
    }
    if (_rank.get(aRoot) >= _rank.get(bRoot)) {
      _rank.put(aRoot, _rank.get(aRoot) + _rank.get(bRoot));
      _parent.put(bRoot, aRoot);
    } else {
      _rank.put(bRoot, _rank.get(bRoot) + _rank.get(aRoot));
      _parent.put(aRoot, bRoot);
    }
  }

  public boolean connected(T a, T b) {
    return findRoot(a).equals(findRoot(b));
  }

  public List<T> getAllMembers() {
    return new ArrayList<>(_parent.keySet());
  }

  public Set<T> getMembers(T a) {
    return _parent.keySet().stream().filter(member -> connected(member, a)).collect(Collectors.toSet());
  }

  public boolean isEmpty() {
    return _rank.isEmpty();
  }
}

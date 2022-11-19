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
import java.util.stream.Collectors;


public class DSU {

  public static DSU of(int a) {
    DSU dsu = new DSU();
    dsu.add(a);
    return dsu;
  }

  public static DSU of(Collection<Integer> members) {
    DSU dsu = new DSU();
    for (Integer member: members) {
      dsu.add(member);
    }
    return dsu;
  }

  private final Map<Integer, Integer> _rank;
  private final Map<Integer, Integer> _parent;

  public DSU() {
    _rank = new HashMap<>();
    _parent = new HashMap<>();
  }

  public boolean contains(int a) {
    return _rank.containsKey(a);
  }

  public void add(int a) {
    if (!_rank.containsKey(a)) {
      _rank.put(a, 1);
      _parent.put(a, a);
    }
  }

  public int findRoot(int a) {
    if (_parent.get(a) == a) {
      return a;
    }
    int newParent = findRoot(_parent.get(a));
    _parent.put(a, newParent);
    return newParent;
  }

  public void merge(int a, int b) {
    int aRoot = findRoot(a);
    int bRoot = findRoot(b);
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

  public boolean connected(int a, int b) {
    return findRoot(a) == findRoot(b);
  }

  public List<Integer> getAllMembers() {
    return new ArrayList<>(_parent.keySet());
  }

  public Set<Integer> getMembers(int a) {
    return _parent.keySet().stream().filter(member -> connected(member, a)).collect(Collectors.toSet());
  }

  public boolean isEmpty() {
    return _rank.isEmpty();
  }
}

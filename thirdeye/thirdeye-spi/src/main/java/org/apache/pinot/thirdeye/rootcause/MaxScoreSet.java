/*
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

package org.apache.pinot.thirdeye.rootcause;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


public class MaxScoreSet<T extends Entity> implements Set<T> {
  private final Map<String, T> delegate = new HashMap<>();

  public MaxScoreSet() {
    // left blank
  }

  public MaxScoreSet(Collection<T> entities) {
    this.addAll(entities);
  }

  @Override
  public int size() {
    return this.delegate.size();
  }

  @Override
  public boolean isEmpty() {
    return this.delegate.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    if (!(o instanceof Entity))
      return false;
    final Entity e = (Entity) o;
    final String urn = e.getUrn();

    if (!this.delegate.containsKey(urn))
      return false;
    return this.delegate.get(urn).equals(e);
  }

  @Override
  public Iterator<T> iterator() {
    return this.delegate.values().iterator();
  }

  @Override
  public Object[] toArray() {
    return this.delegate.values().toArray();
  }

  @Override
  public <T1> T1[] toArray(T1[] a) {
    return this.delegate.values().toArray(a);
  }

  @Override
  public boolean remove(Object o) {
    if (!(o instanceof Entity))
      return false;
    final Entity e = (Entity) o;
    final String urn = e.getUrn();

    if (!this.delegate.containsKey(urn))
      return false;
    if (!this.delegate.get(urn).equals(e))
      return false;

    this.delegate.remove(urn);
    return true;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    for (Object o : c) {
      if (!this.contains(o)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    boolean changed = false;
    for (T e : c) {
      changed |= this.add(e);
    }
    return changed;
  }

  @Override
  public boolean add(T t) {
    final String urn = t.getUrn();
    if (!this.delegate.containsKey(urn) || this.delegate.get(urn).getScore() < t.getScore()) {
      this.delegate.put(urn, t);
      return true;
    }
    return false;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    Map<String, Entity> valid = new HashMap<>();
    for (Object o : c) {
      if (!(o instanceof Entity))
        continue;
      final Entity e = (Entity) o;
      valid.put(e.getUrn(), e);
    }

    boolean changed = false;
    final Iterator<Map.Entry<String, T>> it = this.delegate.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, T> entry = it.next();
      final String urn = entry.getKey();
      final Entity entity = entry.getValue();

      if (!valid.containsKey(urn) || !valid.get(urn).equals(entity)) {
        it.remove();
        changed = true;
      }
    }

    return changed;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    boolean changed = false;
    for (Object o : c) {
      changed |= this.remove(o);
    }
    return changed;
  }

  @Override
  public void clear() {
    this.delegate.clear();
  }

  @Override
  public String toString() {
    return this.delegate.values().toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MaxScoreSet)) {
      return false;
    }
    MaxScoreSet<?> that = (MaxScoreSet<?>) o;
    return Objects.equals(delegate, that.delegate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(delegate);
  }
}

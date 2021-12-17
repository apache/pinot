/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.segment.local.utils.nativefst.mutablefst;

import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.ObjectIntCursor;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import utils.FstUtils;
import java.util.Iterator;


/**
 * A symbol table composed of a (initially empty) mutable symbol table and a backing
 * symbol table that is treated as an immutable table (regardless if its actually mutable or not)
 * this allows you to make symbol tables that are backed by a copy but actually might add some of its
 * own mutable elements
 *
 * NOTE undefined behavior if the backing symboltable is modified after being used to back a union
 * table; in particular there will be duplicate ids assigned which would be very bad
 *
 * NOTE this is not thread safe
 * @author Atri Sharma
 */
public class UnionSymbolTable implements WriteableSymbolTable {

  public static UnionSymbolTable copyFrom(UnionSymbolTable union) {
    if (union.filter == null) {
      return new UnionSymbolTable(null, union.backingMaxId, union.backing);
    }
    return new UnionSymbolTable(new MutableSymbolTable(union.filter), union.backingMaxId, union.backing);
  }

  private MutableSymbolTable filter = null; // initially null lazily create the first time you need it
  private final SymbolTable backing;
  private final int backingMaxId;
  private final InvertedSymbolTable inverted = new InvertedSymbolTable() {
    @Override
    public String keyForId(int id) {
      if (filter != null && id > backingMaxId) {
        return filter.invert().keyForId(id);
      }
      return backing.invert().keyForId(id);
    }

    @Override
    public boolean containsKey(int id) {
      if (filter != null && filter.invert().containsKey(id)) {
        return true;
      }
      return backing.invert().containsKey(id);
    }
  };

  public UnionSymbolTable(SymbolTable backing) {
    this.backing = backing;
    this.backingMaxId = AbstractSymbolTable.maxIdIn(backing);
  }

  protected UnionSymbolTable(MutableSymbolTable filter, int backingMaxId, SymbolTable backing) {
    this.filter = filter;
    this.backingMaxId = backingMaxId;
    this.backing = backing;
  }

  @Override
  public int size() {
    if (filter == null) {
      return backing.size();
    }
    return filter.size() + backing.size();
  }

  @Override
  public Iterator<ObjectIntCursor<String>> iterator() {
    if (filter == null) {
      return backing.iterator();
    }
    return Iterators.concat(backing.iterator(), filter.iterator());
  }

  @Override
  public Iterable<IntCursor> indexes() {
    if (filter == null) {
      return backing.indexes();
    }
    return Iterables.concat(backing.indexes(), filter.indexes());
  }

  @Override
  public Iterable<String> symbols() {
    if (filter == null) {
      return backing.symbols();
    }
    return Iterables.concat(backing.symbols(), filter.symbols());
  }

  @Override
  public int get(String symbol) {
    if (filter != null) {
      try {
        return filter.get(symbol);
      } catch (IllegalArgumentException e) {
        // this didn't have it
      }
    }
    return backing.get(symbol);
  }

  @Override
  public boolean contains(String symbol) {
    if (filter != null && filter.contains(symbol)) {
      return true;
    }
    return backing.contains(symbol);
  }

  @Override
  public InvertedSymbolTable invert() {
    return inverted;
  }

  @Override
  public int getOrAdd(String symbol) {
    if (backing.contains(symbol)) {
      return backing.get(symbol);
    }
    initFilter();
    return filter.getOrAdd(symbol);
  }

  private void initFilter() {
    if (filter == null) {
      filter = new MutableSymbolTable(backingMaxId + 1);
    }
  }

  @Override
  public void put(String symbol, int id) {
    initFilter();
    filter.put(symbol, id);
  }

  @Override
  public int hashCode() {
    throw new IllegalStateException("cant use as a key");
  }

  @Override
  public boolean equals(Object obj) {
    return FstUtils.symbolTableEquals(this, obj);
  }
}

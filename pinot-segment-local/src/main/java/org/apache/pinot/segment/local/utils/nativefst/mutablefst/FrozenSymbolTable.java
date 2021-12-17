/*
 *
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.apache.pinot.segment.local.utils.nativefst.mutablefst;

import java.util.Iterator;


/**
 * Adapter/decorator for a symbol table (any kind) that presents a writeable symbol table interface but throws
 * exceptions if you actually try to write to it
 */
public class FrozenSymbolTable implements WriteableSymbolTable {

  /**
   * Freeze the given symbol table by decorating it (or just returning if its already frozen)
   * @param symbolTable
   * @return
   */
  public static FrozenSymbolTable freeze(SymbolTable symbolTable) {
    if (symbolTable instanceof FrozenSymbolTable) {
      return (FrozenSymbolTable) symbolTable;
    }
    return new FrozenSymbolTable(symbolTable);
  }

  private final SymbolTable backing;

  public FrozenSymbolTable(SymbolTable backing) {
    this.backing = backing;
  }

  @Override
  public int size() {
    return backing.size();
  }

  @Override
  public Iterator<ObjectIntCursor<String>> iterator() {
    return backing.iterator();
  }

  @Override
  public Iterable<IntCursor> indexes() {
    return backing.indexes();
  }

  @Override
  public Iterable<String> symbols() {
    return backing.symbols();
  }

  @Override
  public int get(String symbol) {
    return backing.get(symbol);
  }

  @Override
  public boolean contains(String symbol) {
    return backing.contains(symbol);
  }

  @Override
  public InvertedSymbolTable invert() {
    return backing.invert();
  }

  @Override
  public int getOrAdd(String symbol) {
    try {
      return backing.get(symbol);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("You cannot add a new symbol to a frozen symbol table", e);
    }
  }

  @Override
  public void put(String symbol, int id) {
    throw new IllegalArgumentException("You cannot put a mapping in a frozen symbol table; trying to add " +
        symbol + " -> " + id);
  }
}

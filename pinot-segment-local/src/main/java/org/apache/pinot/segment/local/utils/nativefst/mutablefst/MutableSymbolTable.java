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

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.List;


/**
 * A mutable symbol table to record mappings between symbols and ids; This is the typical
 * implementation of a WriteableSymbolTable (yes the naming is unfortunate)
 *
 * @author Atri Sharma
 */
public class MutableSymbolTable extends AbstractSymbolTable implements WriteableSymbolTable {

  // the next available id in the symbol table
  private int nextId;

  public MutableSymbolTable() {
    this.nextId = 0;
  }

  /**
   * Construct a new mutable symbol table from the given symbol table
   * @param copyFrom
   */
  public MutableSymbolTable(SymbolTable copyFrom) {
    super(copyFrom);

    if (copyFrom instanceof MutableSymbolTable) {
      this.nextId = ((MutableSymbolTable)copyFrom).nextId;
    } else {
      this.nextId = AbstractSymbolTable.maxIdIn(copyFrom) + 1;
    }
  }

  private void putMappingOrThrow(String symbol, int id) {
    if (symbolToId.putIfAbsent(symbol, id) != symbolToId.defaultReturnValue()) {
      if (symbolToId.get(symbol) != id) {
        throw new IllegalArgumentException("Putting a contradictory mapping of " + symbol + " to " + id + " when its"
                                           + " already " + symbolToId.get(symbol));
      }
      return; // already have the mapping and we silently drop dups
    }
    if (idToSymbol.putIfAbsent(id, symbol) != idToSymbol.defaultReturnValue()) {
      throw new IllegalStateException("Somehow the id->symbol table is wrong for " + symbol + " to " + id + " got " +
                                      idToSymbol.get(id));
    }
  }

  /**
   * Remove the mapping for the given id. Note that the newly assigned 'next' ids are monotonically
   * increasing, so removing one id does not free it up to be assigned in future symbol table adds; there
   * will just be holes in the symbol mappings
   * @see #trimIds() for a way to compact the assigned ids
   * @param id
   */
  public void remove(int id) {
    String symbol = invert().keyForId(id);
    idToSymbol.remove(id);
    symbolToId.remove(symbol);
  }

  /**
   * If there are ids to reclaim at the end, then this will do this. Certainly be careful if you are
   * doing operations where it is expected that the id mappings will be consistent across multiple FSTs
   * such as in compose where you want the output of A to be equal to the input of B
   */
  public void trimIds() {
    // typical case shortcut
    if (idToSymbol.containsKey(nextId - 1)) {
      return;
    }
    int max = -1;
    for (ObjectIterator<Int2ObjectMap.Entry<String>> it = idToSymbol.int2ObjectEntrySet().iterator(); it.hasNext(); ) {
      Int2ObjectMap.Entry<String> cursor = it.next();
      max = Math.max(max, cursor.getIntKey());
    }
    nextId = max + 1;
  }

  /**
   * Remap a set of oldIndex -> newIndex such that this whole thing is done as one operation and you dont have
   * to worry about the ordering to consider to be sure you dont lose any symbols
   * Each indexPair is <oldId, newId>
   * @param listOfOldToNew
   */
  public void remapAll(List<IndexPair> listOfOldToNew) {
    List<String> symbols = Lists.newArrayListWithCapacity(listOfOldToNew.size());
    int max = -1;
    for (int i = 0; i < listOfOldToNew.size(); i++) {
      IndexPair indexPair = listOfOldToNew.get(i);
      symbols.add(invert().keyForId(indexPair.getLeft()));
      max = Math.max(max, indexPair.getRight());
    }
    if (max >= nextId) {
      nextId = max + 1;
    }
    // now actually remap them
    for (int i = 0; i < listOfOldToNew.size(); i++) {
      IndexPair pair = listOfOldToNew.get(i);
      String symbol = symbols.get(i);
      idToSymbol.remove(pair.getLeft());
      symbolToId.remove(symbol);

      idToSymbol.put(pair.getRight(), symbol);
      symbolToId.put(symbol, pair.getRight());
    }
  }

  @Override
  public int getOrAdd(String symbol) {
    int thisId = nextId;
    if (symbolToId.putIfAbsent(symbol, thisId) != symbolToId.defaultReturnValue()) {
      nextId += 1;
      if (idToSymbol.putIfAbsent(thisId, symbol) == idToSymbol.defaultReturnValue()) {
        throw new IllegalStateException("idToSymbol is inconsistent for " + symbol);
      }
      return thisId;
    } else {
      return symbolToId.get(symbol);
    }
  }

  @Override
  public void put(String symbol, int id) {
    putMappingOrThrow(symbol, id);
    if (id >= nextId) {
      nextId = id + 1;
    }
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public Object2IntMap.Entry<String> next() {
    return null;
  }
}

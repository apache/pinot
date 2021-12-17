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

/**
 * contract for a symbol table that can be written into (I know the name choice of Writeable vs
 * Mutable sucks)
 * @author Atri Sharma
 */
public interface WriteableSymbolTable extends SymbolTable {

  /**
   * Return the id of the mapping for the given symbol or add the symbol as a new mapping and
   * return the newly assigned id
   * @param symbol
   * @return
   */
  int getOrAdd(String symbol);

  /**
   * This puts the given mapping in with the specified id; you cannot use this to overwrite mappings.
   * If you call this and it tries to do that, it will throw an IllegalArgumentException. Overwriting
   * mappings should be done via `remove()` then `getOrAdd()`.  This is only useful in small circumstances
   * where you need to ensure that the mappings between two tables are the same.
   * @param symbol
   * @param id the next mapping assigned by this symbol table will be updated to respect this
   */
  void put(String symbol, int id);

}

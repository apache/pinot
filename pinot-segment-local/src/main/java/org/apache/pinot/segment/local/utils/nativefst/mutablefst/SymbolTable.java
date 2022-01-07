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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Iterator;


/**
 * Base interface for a symbol table which is a mapping of symbols to integers
 * @author Atri Sharma
 */
public interface SymbolTable extends ObjectIterator<Object2IntMap.Entry<String>> {

  /**
   * How many symbol mappings are in this table
   * @return
   */
  int size();

  /**
   * Produces an iterator over all string -> int mappings in the symbol table.
   * NOTE that as typical in carrotsearch maps, this is an iterator of a cursor object which is the
   * same cursor object returned in each invocation of `next()`. Do NOT let the cursor escape the
   * local scope as it will change the value at each step of iteration
   * @return
   */
  ObjectIterator<Object2IntMap.Entry<String>> iterator();

  /**
   * Return the index of this symbol in this symbol table or throw IllegalArgumentException if no
   * such symbol mapping is present
   * @param symbol
   * @return
   */
  int get(String symbol);

  /**
   * Return true if this symbol table contains the given symbol
   * @param symbol
   * @return
   */
  boolean contains(String symbol);

  /**
   * Returns the inverted symbol table which is a mapping of int -> symbol (string).
   * NOTE that this instance is tied to this symbol table, not recreated each time `invert()`
   * is called. Thus, it can be effeciently used just as `table.invert().keyForId(..)`
   * @return
   */
  InvertedSymbolTable invert();

  /**
   * In an effort to reduce confusion over the mappings of string -> id or co-mappings of id -> string
   * the id -> string operations are segregated into a separate interface: InvertedSymbolTable
   */
  interface InvertedSymbolTable {

    /**
     * Looks up the key for this id and throws an exception if it cant find it
     * @param id the id to lookup
     */
    String keyForId(int id);

    /**
     * Returns true if this symbol table contains a mapping for id
     * @param id
     * @return
     */
    boolean containsKey(int id);
  }


}

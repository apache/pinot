/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.util;

/**
 * Interface for a map from Key to auto-generated contiguous integer Id's.
 *
 * @param <Key>
 */
public interface IdMap<Key> {
  int INVALID_ID = -1;

  /**
   * Puts the specified key into the map. The id for the key is auto-generated as follows:
   *
   * <ul> If key does not exist in the map, it is inserted into the map with a new id which is equal to number of
   * elements in the map before the key is inserted. For example, the first key inserted will have id of 0, second
   * will have an id of 1, and so on. </ul>
   *
   * <ul> If key already exists in the map, then it keeps its original id.</ul>
   *
   * @param key Key to be inserted into the map.
   *
   * @return Returns the id for the key.
   */
  int put(Key key);

  /**
   * Returns the id associated with the specified key.
   *
   * <ul> Returns the id of the key if it exists. </ul>
   * <ul> Returns {@link IdMap#INVALID_ID} if the key does not exist in the map. </ul>
   *
   * @param key Key to get.
   * @return Id of the key.
   */
  int getId(Key key);


  /**
   * Returns the key associated with the specified id.
   *
   * <ul> Returns the id of the key if it exists. </ul>
   * <ul> Returns null if the key does not exist in the map. </ul>
   *
   * @param id id for which to get the key.
   * @return Key for the id.
   */
  Key getKey(int id);


  /**
   * Returns the current size of the map, zero if the map is empty.
   * @return Size of the map.
   */
  int size();

  /**
   * Clears all contents of the map.
   */
  void clear();
}

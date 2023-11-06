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
package org.apache.pinot.core.query.aggregation.groupby;

import java.util.Iterator;
import org.apache.pinot.core.operator.blocks.ValueBlock;


/**
 * Interface for generating group keys.
 */
public interface GroupKeyGenerator {
  char DELIMITER = '\0';
  int INVALID_ID = -1;

  /**
   * Get the global upper bound of the group key. All group keys generated or will be generated should be less than this
   * value. This interface can be called before generating group keys to determine the type and size of the value result
   * holder.
   *
   * @return global upper bound of the group key.
   */
  int getGlobalGroupKeyUpperBound();

  /**
   * Returns whether the globalGroupKeyUpperBound was reached while processing the segment. This is used to indicate
   * that the group by might not have perfectly accurate results.
   * @return
   */
  boolean globalGroupKeyLimitReached();

  /**
   * Generates group keys on the given value block and returns the result to the given buffer.
   * <p>This method is for situation where all the group-by columns are single-valued.
   *
   * @param valueBlock Value block
   * @param groupKeys  Buffer to return the results
   */
  void generateKeysForBlock(ValueBlock valueBlock, int[] groupKeys);

  /**
   * Generate group keys on the given value block and returns the result to the given buffer.
   * <p>This method is for situation where at least one group-by columns are multi-valued.
   *
   * @param valueBlock Value block
   * @param groupKeys  Buffer to return the results
   */
  void generateKeysForBlock(ValueBlock valueBlock, int[][] groupKeys);

  /**
   * Get the current upper bound of the group key. All group keys already generated should be less than this value. This
   * interface can be called after generating some group keys and before processing them to determine whether to expand
   * the size of the value result holder.
   *
   * @return current upper bound of the group key.
   */
  int getCurrentGroupKeyUpperBound();

  /**
   * Returns an iterator of {@link GroupKey}. Use this interface to iterate through all the group keys.
   */
  Iterator<GroupKey> getGroupKeys();

  /**
   * Return current number of unique keys
   */
  int getNumKeys();

  /**
   * This class encapsulates the integer group id and the group keys.
   */
  class GroupKey {
    public int _groupId;
    public Object[] _keys;
  }
}

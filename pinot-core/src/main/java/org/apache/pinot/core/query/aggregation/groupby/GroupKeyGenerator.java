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
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.core.operator.blocks.TransformBlock;


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
   * Generates group keys on the given transform block and returns the result to the given buffer.
   * <p>This method is for situation where all the group-by columns are single-valued.
   *
   * @param transformBlock Transform block
   * @param groupKeys Buffer to return the results
   */
  void generateKeysForBlock(TransformBlock transformBlock, int[] groupKeys);

  /**
   * Generate group keys on the given transform block and returns the result to the given buffer.
   * <p>This method is for situation where at least one group-by columns are multi-valued.
   *
   * @param transformBlock Transform block
   * @param groupKeys Buffer to return the results
   */
  void generateKeysForBlock(TransformBlock transformBlock, int[][] groupKeys);

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
   * Returns an iterator of {@link StringGroupKey}. Use this interface to iterate through all the group keys.
   * TODO: Remove this interface after deprecating PQL.
   */
  Iterator<StringGroupKey> getStringGroupKeys();

  /**
   * Return current number of unique keys
   */
  int getKeyNum();

  /**
   * This class encapsulates the integer group id and the group keys.
   */
  class GroupKey {
    public int _groupId;
    public Object[] _keys;
  }

  /**
   * This class encapsulates the integer group id and the string group key.
   */
  class StringGroupKey {
    public int _groupId;
    public String _stringKey;

    /**
     * Returns the group keys as a String array.
     * NOTE: DO NOT use this method for single group-by expression, directly use _stringKey instead for performance
     *       concern.
     */
    public String[] getKeys() {
      assert _stringKey.indexOf(DELIMITER) != -1;
      // Preserve all tokens to support empty strings
      return StringUtils.splitPreserveAllTokens(_stringKey, GroupKeyGenerator.DELIMITER);
    }
  }
}

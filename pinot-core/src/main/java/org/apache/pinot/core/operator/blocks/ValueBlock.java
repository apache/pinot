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
package org.apache.pinot.core.operator.blocks;

import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.BlockValSet;


/**
 * The {@code ValueBlock} contains a block of values for multiple expressions.
 */
public interface ValueBlock extends Block {

  /**
   * Returns the number of documents within the block.
   */
  int getNumDocs();

  /**
   * Returns the document ids from the segment, or {@code null} if it is not available.
   */
  @Nullable
  int[] getDocIds();

  /**
   * Returns the values for a given expression.
   */
  BlockValSet getBlockValueSet(ExpressionContext expression);

  /**
   * Returns the values for a given column (identifier).
   */
  BlockValSet getBlockValueSet(String column);
}

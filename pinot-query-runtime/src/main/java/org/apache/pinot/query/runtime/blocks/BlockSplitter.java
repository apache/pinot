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
package org.apache.pinot.query.runtime.blocks;

import com.google.common.collect.Iterators;
import java.util.Iterator;


/**
 * Interface for splitting transferable blocks. This is used for ensuring
 * that the blocks that are sent along the wire play nicely with the
 * underlying transport.
 */
public interface BlockSplitter {
  BlockSplitter NO_OP = (block, maxBlockSize) -> Iterators.singletonIterator(block);

  /**
   * @return a list of blocks that was split from the original {@code block}
   */
  Iterator<TransferableBlock> split(TransferableBlock block, int maxBlockSize);
}

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
package org.apache.pinot.core.query.aggregation.groupby.offheap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// [GroupKeyGenerator] wrapper that owns every off-heap resource created for one group-by execution: the wrapped
/// generator's own off-heap key table (released through the delegate's `close()`) plus any registered off-heap
/// result holders. The existing operator-level `GroupKeyGenerator.close()` call sites (segment trim/sort paths,
/// combine operators, exception guards) thus release all off-heap memory without knowing about holders.
///
/// All [GroupKeyGenerator] methods delegate as-is; delegation happens at block granularity, so the extra
/// virtual call is not on the per-row hot path. `close()` is idempotent and closes the delegate first, then every
/// registered resource, attempting all of them even if some fail.
///
/// Not thread-safe: intended for the single-threaded per-segment group-by execution, mirroring the wrapped
/// generator. In the filtered-aggregation case the same instance is shared sequentially across executors and closed
/// exactly once by the operator.
@NotThreadSafe
public class ResourceTrackingGroupKeyGenerator implements GroupKeyGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceTrackingGroupKeyGenerator.class);

  private final GroupKeyGenerator _delegate;
  private final List<AutoCloseable> _resources = new ArrayList<>();
  private boolean _closed;

  public ResourceTrackingGroupKeyGenerator(GroupKeyGenerator delegate) {
    _delegate = delegate;
  }

  /// Registers an off-heap resource to be released when this generator is closed.
  public void register(AutoCloseable resource) {
    _resources.add(resource);
  }

  @Override
  public int getGlobalGroupKeyUpperBound() {
    return _delegate.getGlobalGroupKeyUpperBound();
  }

  @Override
  public void generateKeysForBlock(ValueBlock valueBlock, int[] groupKeys) {
    _delegate.generateKeysForBlock(valueBlock, groupKeys);
  }

  @Override
  public void generateKeysForBlock(ValueBlock valueBlock, int[][] groupKeys) {
    _delegate.generateKeysForBlock(valueBlock, groupKeys);
  }

  @Override
  public int getCurrentGroupKeyUpperBound() {
    return _delegate.getCurrentGroupKeyUpperBound();
  }

  @Override
  public Iterator<GroupKey> getGroupKeys() {
    return _delegate.getGroupKeys();
  }

  @Override
  public int getNumKeys() {
    return _delegate.getNumKeys();
  }

  @Override
  public void close() {
    if (_closed) {
      return;
    }
    _closed = true;
    try {
      _delegate.close();
    } catch (Exception e) {
      LOGGER.warn("Caught exception while closing group key generator: {}", _delegate.getClass().getName(), e);
    }
    for (AutoCloseable resource : _resources) {
      try {
        resource.close();
      } catch (Exception e) {
        LOGGER.warn("Caught exception while closing off-heap group-by resource: {}", resource.getClass().getName(), e);
      }
    }
  }
}

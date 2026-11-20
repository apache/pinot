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
package org.apache.pinot.integration.tests.models;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.local.upsert.BasePartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.ConcurrentMapTableUpsertMetadataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertTrue;


/// Test manager that delays one immutable segment before registering its real upsert metadata. The superclass owns
/// the real partition managers and their lifecycle; concurrent callers use forwarding mocks only to intercept loading.
public class DelayedTableUpsertMetadataManager extends ConcurrentMapTableUpsertMetadataManager {
  private final Map<Integer, BasePartitionUpsertMetadataManager> _delegates = new ConcurrentHashMap<>();
  private final CountDownLatch _loadStarted = new CountDownLatch(1);
  private final CountDownLatch _releaseLoad = new CountDownLatch(1);
  private volatile String _delayedSegment;

  public void delaySegment(String segmentName) {
    _delayedSegment = segmentName;
  }

  public boolean awaitDelayedLoad(long timeout, TimeUnit unit)
      throws InterruptedException {
    return _loadStarted.await(timeout, unit);
  }

  public void releaseDelayedLoad() {
    _releaseLoad.countDown();
  }

  @Override
  public BasePartitionUpsertMetadataManager getOrCreatePartitionManager(int partitionId) {
    return _delegates.computeIfAbsent(partitionId, id -> {
      BasePartitionUpsertMetadataManager realManager = super.getOrCreatePartitionManager(id);
      BasePartitionUpsertMetadataManager delegate =
          mock(BasePartitionUpsertMetadataManager.class, delegatesTo(realManager));
      doAnswer(invocation -> {
        ImmutableSegment segment = invocation.getArgument(0);
        if (segment.getSegmentName().equals(_delayedSegment)) {
          _loadStarted.countDown();
          assertTrue(_releaseLoad.await(5, TimeUnit.MINUTES), "Timed out waiting to release " + _delayedSegment);
        }
        realManager.addSegment(segment);
        return null;
      }).when(delegate).addSegment(any(ImmutableSegment.class));
      return delegate;
    });
  }
}

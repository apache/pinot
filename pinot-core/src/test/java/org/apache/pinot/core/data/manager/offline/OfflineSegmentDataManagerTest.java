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
package org.apache.pinot.core.data.manager.offline;

import org.apache.pinot.core.data.manager.OfflineSegmentFetcherAndLoader;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;


public class OfflineSegmentDataManagerTest {
  @Test
  void testLazyLoad() {
    ImmutableSegment segment = mock(ImmutableSegment.class);
    when(segment.getSegmentName()).thenReturn("foo");
    SegmentCacheManager cacheManager = mock(SegmentCacheManager.class);
    OfflineSegmentFetcherAndLoader fetcherAndLoader = mock(OfflineSegmentFetcherAndLoader.class);
    IndexLoadingConfig indexLoadingConfig = mock(IndexLoadingConfig.class);
    ImmutableSegment immutableSegment = mock(ImmutableSegment.class);
    OfflineSegmentDataManager manager =
        new OfflineSegmentDataManager("foo", "bar", fetcherAndLoader, cacheManager, indexLoadingConfig);

    Mockito.when(fetcherAndLoader.fetchAndLoadOfflineSegment("foo", "bar", indexLoadingConfig)).thenReturn(immutableSegment);

    // Should not have materialized yet
    Assert.assertFalse(manager.hasLocalData());

    // Should be able to get name without materializing
    Assert.assertEquals(manager.getSegmentName(), "bar");

    // Should trigger a materialize
    Assert.assertEquals(manager.getSegment(), immutableSegment);
    Assert.assertTrue(manager.hasLocalData());

    // Trigger a dematerialize
    manager.releaseSegment();
    Assert.assertFalse(manager.hasLocalData());
    Mockito.verify(fetcherAndLoader, times(1)).deleteOfflineSegment("foo", "bar");

    // Make sure we loaded the correct segment
    Mockito.verify(fetcherAndLoader, times(1))
        .fetchAndLoadOfflineSegment("foo", "bar", indexLoadingConfig);
  }
}

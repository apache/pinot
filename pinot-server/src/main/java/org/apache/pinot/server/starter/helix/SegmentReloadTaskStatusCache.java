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
package org.apache.pinot.server.starter.helix;

import com.google.common.cache.CacheBuilder;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.codehaus.jackson.annotate.JsonIgnore;


public class SegmentReloadTaskStatusCache {

  public static class SegmentReloadStatusValue {
    private final long _totalSegmentCount;
    private final AtomicLong _successCount;

    SegmentReloadStatusValue(long totalSegmentCount) {
      _totalSegmentCount = totalSegmentCount;
      _successCount = new AtomicLong(0);
    }

    @JsonIgnore
    public void incrementSuccess() {
      _successCount.addAndGet(1);
    }

    public long getTotalSegmentCount() {
      return _totalSegmentCount;
    }

    public long getSuccessCount() {
      return _successCount.get();
    }
  }

  private SegmentReloadTaskStatusCache() {
  }

  private static final ConcurrentMap<String, SegmentReloadStatusValue> SEGMENT_RELOAD_STATUS_MAP =
      CacheBuilder.newBuilder()
      .maximumSize(1000L)
      .<String, SegmentReloadStatusValue>build().asMap();

  public static void addReloadSegmentTask(String id, long totalSegmentCount) {
    SEGMENT_RELOAD_STATUS_MAP.putIfAbsent(id, new SegmentReloadStatusValue(totalSegmentCount));
  }

  public static void incrementSuccess(String id) {
    SEGMENT_RELOAD_STATUS_MAP.get(id).incrementSuccess();
  }

  public static SegmentReloadStatusValue getStatus(String id) {
    return SEGMENT_RELOAD_STATUS_MAP.get(id);
  }
}

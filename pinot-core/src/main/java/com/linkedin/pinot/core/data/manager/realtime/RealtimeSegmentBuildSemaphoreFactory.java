/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.data.manager.realtime;

import com.linkedin.pinot.core.data.manager.config.InstanceDataManagerConfig;
import java.util.concurrent.Semaphore;
import javax.annotation.Nonnull;


/**
 * Factory for creating the semaphore used by realtime segment builds, to ensure max parallel number of segment builds, at instance level
 */
public class RealtimeSegmentBuildSemaphoreFactory {

  private static RealtimeSegmentBuildSemaphoreFactory INSTANCE = null;

  private static Semaphore _segmentBuildSemaphore = null;

  /**
   * Initializes the factory which provides the instance level semaphore for realtime segment builds
   * @param instanceDataManagerConfig
   */
  public static synchronized void init(@Nonnull InstanceDataManagerConfig instanceDataManagerConfig) {
    if (INSTANCE == null) {
      INSTANCE = new RealtimeSegmentBuildSemaphoreFactory(instanceDataManagerConfig);
    }
  }

  private RealtimeSegmentBuildSemaphoreFactory(InstanceDataManagerConfig instanceDataManagerConfig) {
    int maxParallelBuilds = instanceDataManagerConfig.getMaxParallelSegmentBuilds();
    if (maxParallelBuilds > 0) {
      _segmentBuildSemaphore = new Semaphore(maxParallelBuilds, true);
    }
  }

  public static Semaphore getSemaphore() {
    if (_segmentBuildSemaphore == null) {
      throw new IllegalStateException("RealtimeSegmentBuildSemaphoreFactory has not been initialized");
    }
    return _segmentBuildSemaphore;
  }
}

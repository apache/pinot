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
package org.apache.pinot.plugin.minion.tasks;

import java.util.Set;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorRegistry;
import org.apache.pinot.minion.executor.TaskExecutorFactoryRegistry;
import org.apache.pinot.plugin.minion.tasks.convert_to_raw_index.ConvertToRawIndexTaskExecutorFactory;
import org.apache.pinot.plugin.minion.tasks.convert_to_raw_index.ConvertToRawIndexTaskGenerator;
import org.apache.pinot.plugin.minion.tasks.merge_rollup.MergeRollupTaskExecutorFactory;
import org.apache.pinot.plugin.minion.tasks.merge_rollup.MergeRollupTaskGenerator;
import org.apache.pinot.plugin.minion.tasks.purge.PurgeTaskExecutorFactory;
import org.apache.pinot.plugin.minion.tasks.realtime_to_offline_segments.RealtimeToOfflineSegmentsTaskExecutorFactory;
import org.apache.pinot.plugin.minion.tasks.realtime_to_offline_segments.RealtimeToOfflineSegmentsTaskGenerator;
import org.apache.pinot.plugin.minion.tasks.segment_generation_and_push.SegmentGenerationAndPushTaskExecutorFactory;
import org.apache.pinot.plugin.minion.tasks.segment_generation_and_push.SegmentGenerationAndPushTaskGenerator;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TaskRegistryTest {
  @Test
  public void testTaskGeneratorRegistry() {
    Set<Class<?>> classes = TaskGeneratorRegistry.getTaskGeneratorClasses();
    Assert.assertTrue(classes.size() >= 4);
    Assert.assertTrue(classes.contains(ConvertToRawIndexTaskGenerator.class));
    Assert.assertTrue(classes.contains(MergeRollupTaskGenerator.class));
    Assert.assertTrue(classes.contains(SegmentGenerationAndPushTaskGenerator.class));
    Assert.assertTrue(classes.contains(RealtimeToOfflineSegmentsTaskGenerator.class));
  }

  @Test
  public void testTaskExecutorRegistry() {
    Set<Class<?>> classes = TaskExecutorFactoryRegistry.getTaskExecutorFactoryClasses();
    Assert.assertTrue(classes.size() >= 5);
    Assert.assertTrue(classes.contains(ConvertToRawIndexTaskExecutorFactory.class));
    Assert.assertTrue(classes.contains(MergeRollupTaskExecutorFactory.class));
    Assert.assertTrue(classes.contains(PurgeTaskExecutorFactory.class));
    Assert.assertTrue(classes.contains(SegmentGenerationAndPushTaskExecutorFactory.class));
    Assert.assertTrue(classes.contains(RealtimeToOfflineSegmentsTaskExecutorFactory.class));
  }
}

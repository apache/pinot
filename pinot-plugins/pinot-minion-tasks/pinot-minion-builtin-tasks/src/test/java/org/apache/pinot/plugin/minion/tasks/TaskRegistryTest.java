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
import org.apache.pinot.plugin.minion.tasks.mergerollup.MergeRollupTaskExecutorFactory;
import org.apache.pinot.plugin.minion.tasks.mergerollup.MergeRollupTaskGenerator;
import org.apache.pinot.plugin.minion.tasks.purge.PurgeTaskExecutorFactory;
import org.apache.pinot.plugin.minion.tasks.purge.PurgeTaskGenerator;
import org.apache.pinot.plugin.minion.tasks.realtimetoofflinesegments.RealtimeToOfflineSegmentsTaskExecutorFactory;
import org.apache.pinot.plugin.minion.tasks.realtimetoofflinesegments.RealtimeToOfflineSegmentsTaskGenerator;
import org.apache.pinot.plugin.minion.tasks.segmentgenerationandpush.SegmentGenerationAndPushTaskExecutorFactory;
import org.apache.pinot.plugin.minion.tasks.segmentgenerationandpush.SegmentGenerationAndPushTaskGenerator;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


public class TaskRegistryTest {

  @Test
  public void testTaskGeneratorRegistry() {
    Set<Class<?>> classes = TaskGeneratorRegistry.getTaskGeneratorClasses();
    assertTrue(classes.contains(MergeRollupTaskGenerator.class));
    assertTrue(classes.contains(PurgeTaskGenerator.class));
    assertTrue(classes.contains(SegmentGenerationAndPushTaskGenerator.class));
    assertTrue(classes.contains(RealtimeToOfflineSegmentsTaskGenerator.class));
  }

  @Test
  public void testTaskExecutorRegistry() {
    Set<Class<?>> classes = TaskExecutorFactoryRegistry.getTaskExecutorFactoryClasses();
    assertTrue(classes.contains(MergeRollupTaskExecutorFactory.class));
    assertTrue(classes.contains(PurgeTaskExecutorFactory.class));
    assertTrue(classes.contains(SegmentGenerationAndPushTaskExecutorFactory.class));
    assertTrue(classes.contains(RealtimeToOfflineSegmentsTaskExecutorFactory.class));
  }
}

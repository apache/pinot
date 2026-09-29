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
package org.apache.pinot.core.query.scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.testng.annotations.Test;

import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class QuerySchedulerThreadPoolConfigChangeListenerTest {

  @Test
  public void testOnChangeWithRelevantKeys() {
    ResourceManager resourceManager = mock(ResourceManager.class);
    when(resourceManager.getNumQueryRunnerThreads()).thenReturn(4);
    when(resourceManager.getNumQueryWorkerThreads()).thenReturn(8);

    QuerySchedulerThreadPoolConfigChangeListener listener =
        new QuerySchedulerThreadPoolConfigChangeListener(resourceManager);

    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(QuerySchedulerThreadPoolConfigChangeListener.QUERY_RUNNER_THREADS_KEY, "10");
    clusterConfigs.put(QuerySchedulerThreadPoolConfigChangeListener.QUERY_WORKER_THREADS_KEY, "20");

    Set<String> changedConfigs = Set.of(
        QuerySchedulerThreadPoolConfigChangeListener.QUERY_RUNNER_THREADS_KEY,
        QuerySchedulerThreadPoolConfigChangeListener.QUERY_WORKER_THREADS_KEY);

    listener.onChange(changedConfigs, clusterConfigs);

    verify(resourceManager).resizeThreadPools(10, 20);
  }

  @Test
  public void testOnChangeRunnerOnly() {
    ResourceManager resourceManager = mock(ResourceManager.class);
    when(resourceManager.getNumQueryRunnerThreads()).thenReturn(4);
    when(resourceManager.getNumQueryWorkerThreads()).thenReturn(8);

    QuerySchedulerThreadPoolConfigChangeListener listener =
        new QuerySchedulerThreadPoolConfigChangeListener(resourceManager);

    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(QuerySchedulerThreadPoolConfigChangeListener.QUERY_RUNNER_THREADS_KEY, "10");

    Set<String> changedConfigs = Set.of(
        QuerySchedulerThreadPoolConfigChangeListener.QUERY_RUNNER_THREADS_KEY);

    listener.onChange(changedConfigs, clusterConfigs);

    verify(resourceManager).resizeThreadPools(10, 8);
  }

  @Test
  public void testOnChangeWorkerOnly() {
    ResourceManager resourceManager = mock(ResourceManager.class);
    when(resourceManager.getNumQueryRunnerThreads()).thenReturn(4);
    when(resourceManager.getNumQueryWorkerThreads()).thenReturn(8);

    QuerySchedulerThreadPoolConfigChangeListener listener =
        new QuerySchedulerThreadPoolConfigChangeListener(resourceManager);

    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(QuerySchedulerThreadPoolConfigChangeListener.QUERY_WORKER_THREADS_KEY, "20");

    Set<String> changedConfigs = Set.of(
        QuerySchedulerThreadPoolConfigChangeListener.QUERY_WORKER_THREADS_KEY);

    listener.onChange(changedConfigs, clusterConfigs);

    verify(resourceManager).resizeThreadPools(4, 20);
  }

  @Test
  public void testOnChangeUnrelatedKeys() {
    ResourceManager resourceManager = mock(ResourceManager.class);

    QuerySchedulerThreadPoolConfigChangeListener listener =
        new QuerySchedulerThreadPoolConfigChangeListener(resourceManager);

    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put("some.other.config", "value");

    Set<String> changedConfigs = Set.of("some.other.config");

    listener.onChange(changedConfigs, clusterConfigs);

    verify(resourceManager, never()).resizeThreadPools(anyInt(), anyInt());
  }

  @Test
  public void testOnChangeInvalidRunnerValue() {
    ResourceManager resourceManager = mock(ResourceManager.class);
    when(resourceManager.getNumQueryRunnerThreads()).thenReturn(4);
    when(resourceManager.getNumQueryWorkerThreads()).thenReturn(8);

    QuerySchedulerThreadPoolConfigChangeListener listener =
        new QuerySchedulerThreadPoolConfigChangeListener(resourceManager);

    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(QuerySchedulerThreadPoolConfigChangeListener.QUERY_RUNNER_THREADS_KEY, "not_a_number");

    Set<String> changedConfigs = Set.of(
        QuerySchedulerThreadPoolConfigChangeListener.QUERY_RUNNER_THREADS_KEY);

    listener.onChange(changedConfigs, clusterConfigs);

    verify(resourceManager, never()).resizeThreadPools(anyInt(), anyInt());
  }

  @Test
  public void testOnChangeInvalidWorkerValue() {
    ResourceManager resourceManager = mock(ResourceManager.class);
    when(resourceManager.getNumQueryRunnerThreads()).thenReturn(4);
    when(resourceManager.getNumQueryWorkerThreads()).thenReturn(8);

    QuerySchedulerThreadPoolConfigChangeListener listener =
        new QuerySchedulerThreadPoolConfigChangeListener(resourceManager);

    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(QuerySchedulerThreadPoolConfigChangeListener.QUERY_WORKER_THREADS_KEY, "invalid");

    Set<String> changedConfigs = Set.of(
        QuerySchedulerThreadPoolConfigChangeListener.QUERY_WORKER_THREADS_KEY);

    listener.onChange(changedConfigs, clusterConfigs);

    verify(resourceManager, never()).resizeThreadPools(anyInt(), anyInt());
  }

  @Test
  public void testOnChangeDeletedRunnerKeyRevertsToDefault() {
    ResourceManager resourceManager = mock(ResourceManager.class);
    when(resourceManager.getNumQueryRunnerThreads()).thenReturn(32);
    when(resourceManager.getNumQueryWorkerThreads()).thenReturn(8);

    QuerySchedulerThreadPoolConfigChangeListener listener =
        new QuerySchedulerThreadPoolConfigChangeListener(resourceManager);

    Map<String, String> clusterConfigs = new HashMap<>();

    Set<String> changedConfigs = Set.of(
        QuerySchedulerThreadPoolConfigChangeListener.QUERY_RUNNER_THREADS_KEY);

    listener.onChange(changedConfigs, clusterConfigs);

    verify(resourceManager).resizeThreadPools(ResourceManager.DEFAULT_QUERY_RUNNER_THREADS, 8);
  }

  @Test
  public void testOnChangeDeletedWorkerKeyRevertsToDefault() {
    ResourceManager resourceManager = mock(ResourceManager.class);
    when(resourceManager.getNumQueryRunnerThreads()).thenReturn(4);
    when(resourceManager.getNumQueryWorkerThreads()).thenReturn(64);

    QuerySchedulerThreadPoolConfigChangeListener listener =
        new QuerySchedulerThreadPoolConfigChangeListener(resourceManager);

    Map<String, String> clusterConfigs = new HashMap<>();

    Set<String> changedConfigs = Set.of(
        QuerySchedulerThreadPoolConfigChangeListener.QUERY_WORKER_THREADS_KEY);

    listener.onChange(changedConfigs, clusterConfigs);

    verify(resourceManager).resizeThreadPools(4, ResourceManager.DEFAULT_QUERY_WORKER_THREADS);
  }

  @Test
  public void testOnChangeBothKeysDeletedRevertToDefaults() {
    ResourceManager resourceManager = mock(ResourceManager.class);
    when(resourceManager.getNumQueryRunnerThreads()).thenReturn(32);
    when(resourceManager.getNumQueryWorkerThreads()).thenReturn(64);

    QuerySchedulerThreadPoolConfigChangeListener listener =
        new QuerySchedulerThreadPoolConfigChangeListener(resourceManager);

    Map<String, String> clusterConfigs = new HashMap<>();

    Set<String> changedConfigs = Set.of(
        QuerySchedulerThreadPoolConfigChangeListener.QUERY_RUNNER_THREADS_KEY,
        QuerySchedulerThreadPoolConfigChangeListener.QUERY_WORKER_THREADS_KEY);

    listener.onChange(changedConfigs, clusterConfigs);

    verify(resourceManager).resizeThreadPools(
        ResourceManager.DEFAULT_QUERY_RUNNER_THREADS, ResourceManager.DEFAULT_QUERY_WORKER_THREADS);
  }
}

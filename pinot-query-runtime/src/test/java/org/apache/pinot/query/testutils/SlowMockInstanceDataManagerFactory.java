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
package org.apache.pinot.query.testutils;

import java.util.List;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;


/**
 * A {@link MockInstanceDataManagerFactory} variant that sleeps when segments are acquired.
 * <p>This allows integration tests to deterministically trigger server-side timeouts.</p>
 */
public class SlowMockInstanceDataManagerFactory extends MockInstanceDataManagerFactory {
  private final long _sleepMs;

  public SlowMockInstanceDataManagerFactory(String serverName, long sleepMs) {
    super(serverName);
    _sleepMs = sleepMs;
  }

  @Override
  protected void customizeTableDataManager(TableDataManager tableDataManager, String tableNameWithType,
      Answer<List<SegmentDataManager>> acquireSegmentsAnswer) {
    when(tableDataManager.acquireSegments(anyList(), eq(null), anyList())).thenAnswer(invocation -> {
      try {
        Thread.sleep(_sleepMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return acquireSegmentsAnswer.answer(invocation);
    });
  }
}

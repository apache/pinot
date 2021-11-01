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
package org.apache.pinot.common.utils;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * A default {@link ThreadFactory} implementation that accepts the name prefix
 * of the created threads as a constructor argument. Otherwise, this factory
 * yields the same semantics as the thread factory returned by
 * {@link Executors#defaultThreadFactory()}.
 */
public class NamedThreadFactory implements ThreadFactory {
  private static final AtomicInteger THREAD_POOL_NUMBER = new AtomicInteger(1);
  private static final String NAME_PATTERN = "%s-%d-thread";

  private final ThreadGroup _group;
  private final AtomicInteger _threadNumber = new AtomicInteger(1);
  private final String _threadNamePrefix;

  /**
   * Creates a new {@link NamedThreadFactory} instance
   *
   * @param threadNamePrefix the name prefix assigned to each thread created.
   */
  public NamedThreadFactory(String threadNamePrefix) {
    final SecurityManager s = System.getSecurityManager();
    _group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
    _threadNamePrefix =
        String.format(NAME_PATTERN, checkPrefix(threadNamePrefix), THREAD_POOL_NUMBER.getAndIncrement());
  }

  private static String checkPrefix(String prefix) {
    return prefix == null || prefix.isEmpty() ? "Pinot" : prefix;
  }

  /**
   * Creates a new {@link Thread}
   *
   * @see java.util.concurrent.ThreadFactory#newThread(java.lang.Runnable)
   */
  public Thread newThread(Runnable r) {
    final Thread t =
        new Thread(_group, r, String.format("%s-%d", _threadNamePrefix, _threadNumber.getAndIncrement()), 0);
    t.setDaemon(false);
    t.setPriority(Thread.NORM_PRIORITY);
    return t;
  }
}

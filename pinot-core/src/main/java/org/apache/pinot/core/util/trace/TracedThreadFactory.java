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
package org.apache.pinot.core.util.trace;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


public final class TracedThreadFactory implements ThreadFactory {

  private final int _priority;
  private final boolean _daemon;
  private final String _nameFormat;
  private final AtomicInteger _count = new AtomicInteger();

  public TracedThreadFactory(int priority, boolean daemon, String nameFormat) {
    _priority = priority;
    _daemon = daemon;
    _nameFormat = nameFormat;
  }

  @Override
  public Thread newThread(Runnable task) {
    Thread thread = new TracedThread(task);
    thread.setPriority(_priority);
    thread.setDaemon(_daemon);
    thread.setName(String.format(_nameFormat, _count.getAndIncrement()));
    return thread;
  }
}

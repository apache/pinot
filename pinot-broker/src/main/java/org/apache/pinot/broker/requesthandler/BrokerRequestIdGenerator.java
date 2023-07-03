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
package org.apache.pinot.broker.requesthandler;

import java.util.concurrent.atomic.AtomicLong;

/**
 * An ID generator to produce a global unique identifier for each query, used in v1/v2 engine for tracking and
 * inter-stage communication(v2 only). It's guaranteed by:
 * <ol>
 *   <li>
 *     Using a mask computed using the hash-code of the broker-id to ensure two brokers don't arrive at the same
 *     requestId. This mask becomes the most significant 9 digits (in base-10).
 *   </li>
 *   <li>
 *     Using a auto-incrementing counter for the least significant 9 digits (in base-10).
 *   </li>
 * </ol>
 */
public class BrokerRequestIdGenerator {
  private static final long OFFSET = 1_000_000_000L;
  private final long _mask;
  private final AtomicLong _incrementingId = new AtomicLong(0);

  public BrokerRequestIdGenerator(String brokerId) {
    _mask = ((long) (brokerId.hashCode() & Integer.MAX_VALUE)) * OFFSET;
  }

  public long get() {
    long normalized = (_incrementingId.getAndIncrement() & Long.MAX_VALUE) % OFFSET;
    return _mask + normalized;
  }
}

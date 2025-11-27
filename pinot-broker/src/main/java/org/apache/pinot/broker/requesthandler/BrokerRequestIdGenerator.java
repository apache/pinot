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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;


/// An ID generator to produce a global unique identifier for each query, used in single-stage/multi-stage engine for
/// tracking and inter-stage communication (multi-stage engine only). It's guaranteed by:
/// - Using a random number to ensure two brokers (or restarted broker) don't arrive at the same request ID. This random
///   number becomes the most significant 10 digits of the request ID (in base-10).
/// - Using an auto-incrementing counter for the least significant 9 digits (in base-10).
public class BrokerRequestIdGenerator {
  private static final long OFFSET = 1_000_000_000L;
  private final long _base;
  private final AtomicLong _incrementingId = new AtomicLong(0);

  public BrokerRequestIdGenerator() {
    _base = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE) * OFFSET;
  }

  public long get() {
    long normalized = (_incrementingId.getAndIncrement() & Long.MAX_VALUE) % OFFSET;
    return _base + normalized;
  }
}

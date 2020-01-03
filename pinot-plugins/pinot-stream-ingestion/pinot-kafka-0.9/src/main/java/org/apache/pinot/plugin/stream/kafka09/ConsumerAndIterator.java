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
package org.apache.pinot.plugin.stream.kafka09;

import java.util.concurrent.atomic.AtomicLong;
import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;


/**
 * Immutable tuple object for a Kafka consumer and stream iterator.
 */
public class ConsumerAndIterator {
  private final ConsumerConnector _consumer;
  private final ConsumerIterator<byte[], byte[]> _iterator;
  private final long _id;
  private static final AtomicLong idGenerator = new AtomicLong(0L);

  ConsumerAndIterator(final ConsumerConnector consumer, final ConsumerIterator<byte[], byte[]> iterator) {
    _consumer = consumer;
    _iterator = iterator;
    _id = idGenerator.getAndIncrement();
  }

  public ConsumerConnector getConsumer() {
    return _consumer;
  }

  public ConsumerIterator<byte[], byte[]> getIterator() {
    return _iterator;
  }

  public long getId() {
    return _id;
  }

  @Override
  public String toString() {
    return "ConsumerAndIterator{" + "_consumer=" + _consumer + ", _iterator=" + _iterator + ", _id=" + _id + '}';
  }
}

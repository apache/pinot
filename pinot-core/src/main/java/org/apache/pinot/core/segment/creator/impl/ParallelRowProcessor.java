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
package org.apache.pinot.core.segment.creator.impl;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import java.util.concurrent.CountDownLatch;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.GenericRow;

public class ParallelRowProcessor {
    // bufferSize, Needs to fit L3 cache & must be power of 2
    public static final int RING_SIZE = 64;
    private RecordReader _reader;
    private Disruptor<GenericRow> _disruptor;

    /**
     * Initialize a Single Producer & Single Consumer Ring Buffer
     * with `RING_SIZE` entries.
     * 
     * @param reader {@link org.apache.pinot.spi.data.readers.RecordReader} used by producer to read entries from.
     * @param handler {@link com.lmax.disruptor.EventHandler<org.apache.pinot.spi.data.readers.GenericRow>} that will consume entries from the ring buffer.
     */
    public ParallelRowProcessor(RecordReader reader, EventHandler<GenericRow> handler) {
        _reader = reader;

        _disruptor = new Disruptor<GenericRow>(
            GenericRow::new, 
            RING_SIZE, 
            DaemonThreadFactory.INSTANCE,
            ProducerType.SINGLE,
            new BusySpinWaitStrategy());

        _disruptor.handleEventsWith(handler);
    }

    /**
     * Iterates over all elements available in the Reader pushing them into the
     * Ring Buffer.
     * 
     * @throws Exception
     */
    public void run(CountDownLatch start) throws Exception {
      // Start the Disruptor, starts all threads running
      _disruptor.start();

      // Get the ring buffer from the Disruptor to be used for publishing.
      RingBuffer<GenericRow> ringBuffer = _disruptor.getRingBuffer();

      // Iterate over all records pushing them into the ring
      while (_reader.hasNext()) {
        long bufferIndex = ringBuffer.next();
        GenericRow decodedRow = ringBuffer.get(bufferIndex);
        decodedRow.clear();
        _reader.next(decodedRow);
        ringBuffer.publish(bufferIndex);
      }

      // This call is needed to make sure we don't shutdown before the consumer
      // had a chance to start running. This is a rare corner case that only happens
      // when just a few entries are pushed to the ring buffer - e.g., during unit tests.
      start.await();

      // Wait for all entries to be consumed
      _disruptor.shutdown();
    }
}

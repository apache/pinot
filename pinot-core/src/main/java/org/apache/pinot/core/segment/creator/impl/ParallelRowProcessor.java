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

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.GenericRow;

public class ParallelRowProcessor {
    /* bufferSize, Needs to fit L3 cache & must be power of 2 */
    public static final int RING_SIZE = 64;
    private RecordReader reader;
    private Disruptor<GenericRow> disruptor;

    public ParallelRowProcessor(RecordReader reader, EventHandler<GenericRow> handler) {
        this.reader = reader;

        this.disruptor = new Disruptor<GenericRow>(
            GenericRow::new, 
            RING_SIZE, 
            DaemonThreadFactory.INSTANCE,
            ProducerType.SINGLE,
            new BusySpinWaitStrategy());

        this.disruptor.handleEventsWith(handler);
    }

    public void Run() throws Exception {
      // Start the Disruptor, starts all threads running
      disruptor.start();

      // Get the ring buffer from the Disruptor to be used for publishing.
      RingBuffer<GenericRow> ringBuffer = disruptor.getRingBuffer();

      // Iterate over all records pushing them into the ring
      while (reader.hasNext()) {
        long bufferIndex = ringBuffer.next();
        GenericRow decodedRow = ringBuffer.get(bufferIndex);
        decodedRow.clear();
        reader.next(decodedRow);
        ringBuffer.publish(bufferIndex);
      }

      // Wait for all entries to be consumed
      disruptor.shutdown();
    }
}

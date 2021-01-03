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
package org.apache.pinot.plugin.stream.kinesis;

import java.util.List;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.RowMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import software.amazon.awssdk.services.kinesis.model.Record;


public class KinesisRecordsBatch implements MessageBatch<byte[]> {
  private List<Record> _recordList;

  public KinesisRecordsBatch(List<Record> recordList) {
    _recordList = recordList;
  }

  @Override
  public int getMessageCount() {
    return _recordList.size();
  }

  @Override
  public byte[] getMessageAtIndex(int index) {
    return _recordList.get(index).data().asByteArray();
  }

  @Override
  public int getMessageOffsetAtIndex(int index) {
    //TODO: Doesn't translate to offset. Needs to be replaced.
    return _recordList.get(index).hashCode();
  }

  @Override
  public int getMessageLengthAtIndex(int index) {
    return _recordList.get(index).data().asByteArray().length;
  }

  @Override
  public RowMetadata getMetadataAtIndex(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public StreamPartitionMsgOffset getNextStreamParitionMsgOffsetAtIndex(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getNextStreamMessageOffsetAtIndex(int index) {
    throw new UnsupportedOperationException();
  }
}

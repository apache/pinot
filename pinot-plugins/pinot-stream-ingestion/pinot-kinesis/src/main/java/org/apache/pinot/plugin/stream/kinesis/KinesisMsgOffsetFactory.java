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

import java.io.IOException;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;


/**
 * An implementation of the {@link StreamPartitionMsgOffsetFactory} for Kinesis stream
 */
public class KinesisMsgOffsetFactory implements StreamPartitionMsgOffsetFactory {

  @Override
  public void init(StreamConfig streamConfig) {

  }

  @Override
  public StreamPartitionMsgOffset create(String offsetStr) {
    try {
      return new KinesisPartitionGroupOffset(offsetStr);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Caught exception when creating KinesisPartitionGroupOffset from offsetStr: " + offsetStr);
    }
  }

  @Override
  public StreamPartitionMsgOffset create(StreamPartitionMsgOffset other) {
    return new KinesisPartitionGroupOffset(((KinesisPartitionGroupOffset) other).getShardToStartSequenceMap());
  }
}

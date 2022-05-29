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
package org.apache.pinot.plugin.stream.kafka20;

import java.nio.ByteBuffer;
import org.apache.pinot.plugin.stream.kafka.MessageAndOffset;
import org.apache.pinot.spi.stream.RowMetadata;


public class MessageAndOffsetAndMetadata extends MessageAndOffset {
  private final RowMetadata _rowMetadata;

  public MessageAndOffsetAndMetadata(byte[] message, long offset, RowMetadata rowMetadata) {
    super(message, offset);
    _rowMetadata = rowMetadata;
  }

  public MessageAndOffsetAndMetadata(ByteBuffer message, long offset, RowMetadata rowMetadata) {
    super(message, offset);
    _rowMetadata = rowMetadata;
  }

  public RowMetadata getRowMetadata() {
    return _rowMetadata;
  }
}

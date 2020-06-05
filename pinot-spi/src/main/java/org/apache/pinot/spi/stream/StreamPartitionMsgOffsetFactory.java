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
package org.apache.pinot.spi.stream;

import org.apache.pinot.spi.annotations.InterfaceStability;


/**
 * An interface to be implemented by streams if the offset of message in a stream partition
 * is NOT a Long (or int) type.
 * TODO Document the methods after finalizing the interface (Issue 5359)
 * TODO Try to avoid createMaxOffset and createMinOffset methods. (Issue 5359)
 */
@InterfaceStability.Evolving
public interface StreamPartitionMsgOffsetFactory {
  void init(StreamConfig streamConfig);
  StreamPartitionMsgOffset create(String offsetStr);
  StreamPartitionMsgOffset create(StreamPartitionMsgOffset other);
  StreamPartitionMsgOffset createMaxOffset(); // Create an offset that compares highest with all others
  StreamPartitionMsgOffset createMinOffset(); // Create an offset that compares lowest with all others
}

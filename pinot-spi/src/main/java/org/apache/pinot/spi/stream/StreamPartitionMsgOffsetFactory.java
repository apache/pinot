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
 * An interface to be implemented by streams that are consumed using Pinot LLC consumption.
 */
@InterfaceStability.Evolving
public interface StreamPartitionMsgOffsetFactory {
  /**
   * Initialization, called once when the factory is created.
   * @param streamConfig
   */
  void init(StreamConfig streamConfig);

  /**
   * Construct an offset from the string provided.
   * @param offsetStr
   * @return StreamPartitionMsgOffset
   */
  StreamPartitionMsgOffset create(String offsetStr);

  /**
   * Construct an offset from another one provided, of the same type.
   *
   * @param other
   * @return
   */
  StreamPartitionMsgOffset create(StreamPartitionMsgOffset other);
}

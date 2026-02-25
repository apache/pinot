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
package org.apache.pinot.common.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.data.LogicalTableConfig;


/**
 * Pluggable serializer/deserializer for {@link LogicalTableConfig} stored in ZooKeeper as {@link ZNRecord}.
 *
 * <p>Implementations are discovered via {@link java.util.ServiceLoader}. When multiple implementations
 * are present, the one with the highest {@link #getPriority()} wins.</p>
 */
public interface LogicalTableConfigSerDe {

  /**
   * Deserializes a {@link LogicalTableConfig} from the given {@link ZNRecord}.
   */
  LogicalTableConfig fromZNRecord(ZNRecord znRecord)
      throws IOException;

  /**
   * Serializes a {@link LogicalTableConfig} into a {@link ZNRecord}.
   */
  ZNRecord toZNRecord(LogicalTableConfig logicalTableConfig)
      throws JsonProcessingException;

  /**
   * Returns the priority of this implementation. Higher values win over lower values
   * when multiple implementations are discovered via ServiceLoader.
   */
  default int getPriority() {
    return 0;
  }
}

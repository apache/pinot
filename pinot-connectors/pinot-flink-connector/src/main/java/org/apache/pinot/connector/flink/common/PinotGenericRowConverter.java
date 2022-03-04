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
package org.apache.pinot.connector.flink.common;

import java.io.Serializable;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Converter that converts sink input format into Pinot {@link GenericRow}.
 *
 * @param <T> supported sink input format.
 */
public interface PinotGenericRowConverter<T> extends Serializable {

  /**
   * Convert a flink generic type data content to Pinot segment table row.
   *
   * @param value flink type data content
   * @return pinot row
   */
  GenericRow convertToRow(T value);

  /**
   * Convert a Pinot segment table row to flink generic type data content.
   *
   * @param row pinot row
   * @return flink type data content
   */
  T convertFromRow(GenericRow row);
}

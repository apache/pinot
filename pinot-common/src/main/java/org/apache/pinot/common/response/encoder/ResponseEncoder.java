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
package org.apache.pinot.common.response.encoder;

import java.io.IOException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;


public interface ResponseEncoder {

  /**
   * Encode the result table into a byte array.
   * @param resultTable Result table to encode
   * @return Encoded byte array
   */
  byte[] encodeResultTable(ResultTable resultTable, int startRow, int length)
      throws IOException;

  /**
   * Decode the result table from a byte array.
   *
   * @param bytes  Encoded byte array
   * @param rowSize Number of rows in the result table
   * @param schema Schema of the result table
   * @return Decoded result table
   */
  ResultTable decodeResultTable(byte[] bytes, int rowSize, DataSchema schema)
      throws IOException;
}

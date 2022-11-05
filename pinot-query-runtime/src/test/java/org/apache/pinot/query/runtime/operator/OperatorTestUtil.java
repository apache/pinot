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
package org.apache.pinot.query.runtime.operator;

import java.util.List;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public class OperatorTestUtil {
  private OperatorTestUtil() {
  }

  public static final DataSchema TEST_DATA_SCHEMA = new DataSchema(new String[]{"foo", "bar"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

  public static TransferableBlock getEndOfStreamRowBlock() {
    return getEndOfStreamRowBlockWithSchema();
  }

  public static TransferableBlock getEndOfStreamRowBlockWithSchema() {
    return new TransferableBlock(DataBlockUtils.getEndOfStreamDataBlock());
  }

  public static TransferableBlock getRowDataBlock(List<Object[]> rows) {
    return getRowDataBlockWithSchema(rows, TEST_DATA_SCHEMA);
  }

  public static TransferableBlock getRowDataBlockWithSchema(List<Object[]> rows, DataSchema schema) {
    return new TransferableBlock(rows, schema, BaseDataBlock.Type.ROW);
  }
}

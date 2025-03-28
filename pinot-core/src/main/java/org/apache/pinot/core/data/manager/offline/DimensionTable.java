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
package org.apache.pinot.core.data.manager.offline;

import java.io.Closeable;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;


public interface DimensionTable extends Closeable {

  List<String> getPrimaryKeyColumns();

  @Nullable
  FieldSpec getFieldSpecFor(String columnName);

  boolean isEmpty();

  boolean containsKey(PrimaryKey pk);

  /**
   * Deprecated because GenericRow is an inefficient data structure.
   * Use getValue() or getValues() instead.
   * @param pk primary key
   * @return primary key and value as GenericRow.
   */
  @Deprecated
  @Nullable
  GenericRow getRow(PrimaryKey pk);

  @Nullable
  Object getValue(PrimaryKey pk, String columnName);

  @Nullable
  Object[] getValues(PrimaryKey pk, String[] columnNames);
}

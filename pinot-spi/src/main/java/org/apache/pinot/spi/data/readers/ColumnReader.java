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
package org.apache.pinot.spi.data.readers;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nullable;


/**
 * The <code>ColumnReader</code> interface is used to read column data from various data sources
 * for columnar segment building. Unlike RecordReader which reads row-by-row, ColumnReader provides
 * column-wise access to data, enabling efficient columnar segment creation.
 *
 * <p>This interface follows an iterator pattern similar to RecordReader:
 * <ul>
 *   <li>Sequential iteration over all values in a column using hasNext() and next()</li>
 *   <li>Rewind capability to restart iteration</li>
 *   <li>Resource cleanup</li>
 * </ul>
 *
 * <p>Implementations should handle data type conversions, default values for new columns,
 * and efficient column-wise data access patterns.
 */
public interface ColumnReader extends Closeable, Serializable {

  /**
   * Return <code>true</code> if more values remain to be read in this column.
   * <p>This method should not throw exception. Caller is not responsible for handling exceptions from this method.
   */
  boolean hasNext();

  /**
   * Get the next value in the column.
   * <p>This method should be called only if {@link #hasNext()} returns <code>true</code>. Caller is responsible for
   * handling exceptions from this method and skip the value if user wants to continue reading the remaining values.
   *
   * @return Next column value, or null if the value is null
   * @throws IOException If an I/O error occurs while reading
   */
  @Nullable
  Object next()
      throws IOException;


  /**
   * Rewind the reader to start reading from the first value again.
   *
   * @throws IOException If an I/O error occurs while rewinding
   */
  void rewind()
      throws IOException;

  /**
   * Get the name of the column.
   *
   * @return Column name
   */
  String getColumnName();
}

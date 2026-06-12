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
import javax.annotation.Nullable;
import org.roaringbitmap.RoaringBitmap;


/**
 * <p>
 * This interface is designed to support the creation of multiple {@link ColumnReaderFactory} instances
 * from the same underlying data source. This capability is required because different processing contexts
 * may require distinct column reader objects for the same input file and column. For example:
 * <ul>
 *   <li>Different processing stages may need independent iterators over the same column</li>
 *   <li>Parallel processing threads may each require their own column readers</li>
 * </ul>
 * <p>
 */
public interface ColumnarDataSource extends Closeable {

  /**
   * Returns the total number of documents (rows) in this data source.
   *
   * @return the total document count
   */
  int getTotalDocs();

  /**
   * Creates a new {@link ColumnReaderFactory} instance for this data source.
   * <p>
   * Multiple factories can be created from the same data source, allowing different consumers
   * to independently read and process the columnar data. Each factory can then create its own
   * set of column readers without interfering with readers created by other factories.
   *
   * @return a new column reader factory instance
   * @throws IOException if an I/O error occurs while creating the factory
   */
  ColumnReaderFactory createColumnReaderFactory()
      throws IOException;

  /**
   * Returns a string description of the underlying data source.
   * <p>
   * This typically includes information such as the file name, path, or other identifiers
   * that help identify the source of the data.
   *
   * @return a description of the underlying source (e.g., file name)
   */
  String toString();


  /**
   * Returns the valid document IDs bitmap for this data source.
   * Documents not in this bitmap will be filtered out during processing.
   * <p>
   * This is used for scenarios like upsert compaction where only valid (non-obsolete)
   * documents should be processed.
   *
   * @return RoaringBitmap of valid doc IDs, or null if all docs are valid
   */
  @Nullable
  default RoaringBitmap getValidDocIds() {
    return null;
  }
}

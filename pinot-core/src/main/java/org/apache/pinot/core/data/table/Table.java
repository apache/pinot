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
package org.apache.pinot.core.data.table;

import java.util.Iterator;
import org.apache.pinot.common.utils.DataSchema;


/**
 * A container for {@link Record}s
 */
public interface Table {

  /**
   * Update the table with the given record
   */
  boolean upsert(Record record);

  /**
   * Update the table with the given record, indexed on Key
   */
  boolean upsert(Key key, Record record);

  /**
   * Merge all records from given table
   */
  boolean merge(Table table);

  /**
   * Returns the size of the table
   */
  int size();

  /**
   * An iterator for the {@link Record}s in the table
   */
  Iterator<Record> iterator();

  /**
   * Finish any pre exit processing
   * @param sort sort the final results if true
   */
  void finish(boolean sort);

  /**
   * Returns the data schema of the table
   */
  DataSchema getDataSchema();
}

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

import javax.annotation.Nullable;


/**
 * Helper class to store a subset of Record fields
 * IntermediateRecord is derived from a Record
 * Some of the main properties of an IntermediateRecord are:
 *
 * 1. Key in IntermediateRecord is expected to be identical to the one in the Record
 * 2. For values, IntermediateRecord should only have the columns needed for order by
 * 3. Inside the values, the columns should be ordered by the order by sequence
 * 4. For order by on aggregations, final results are extracted
 * 5. There is a mandatory field to store the original record to prevent from duplicate looking up
 */
public class IntermediateRecord {
  public final Key _key;
  public final Comparable[] _values;
  public final Record _record;

  IntermediateRecord(Key key, Comparable[] values, Record record) {
    _key = key;
    _values = values;
    _record = record;
  }

}

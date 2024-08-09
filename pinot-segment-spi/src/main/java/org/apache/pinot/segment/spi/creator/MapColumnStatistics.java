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
package org.apache.pinot.segment.spi.creator;

import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * An interface to read the map column statistics from statistics collectors.
 */
public interface MapColumnStatistics extends ColumnStatistics {

  Object getMinValueForKey(String key);

  Object getMaxValueForKey(String key);

  int getLengthOfShortestElementForKey(String key);

  int getLengthOfLargestElementForKey(String key);

  Set<Pair<String, FieldSpec.DataType>> getKeys();

  boolean isSortedForKey(String key);

  int getTotalNumberOfEntriesForKey(String key);
}

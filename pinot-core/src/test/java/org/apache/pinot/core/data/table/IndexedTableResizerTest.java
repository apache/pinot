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

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.testng.annotations.Test;


/**
 * Tests the functionality of {@link @IndexedTableResizer}
 */
public class IndexedTableResizerTest {

  @Test
  public void testOrderedResize() {
    DataSchema dataSchema = null;
    List<AggregationInfo> aggregationInfos = null;
    List<SelectionSort> selectionSort = null;

    IndexedTableResizer orderedResizer = new OrderedIndexedTableResizer(dataSchema, aggregationInfos, selectionSort);

    Map<Key, Record> recordsMap = null;
    int trimToSize = 0;
    orderedResizer.resizeRecordsMap(recordsMap, trimToSize);

    // verify survivors after resize, for various combinations of selection sort
    // small data sizes, only meant to be functional tests
  }

  @Test
  public void testOrderedResizerSort() {
    DataSchema dataSchema = null;
    List<AggregationInfo> aggregationInfos = null;
    List<SelectionSort> selectionSort = null;

    IndexedTableResizer orderedResizer = new OrderedIndexedTableResizer(dataSchema, aggregationInfos, selectionSort);

    Map<Key, Record> recordsMap = null;
    List<Record> sortedRecords = null;

    sortedRecords = orderedResizer.sortRecordsMap(recordsMap);

    // verify results sorted by order
  }

  @Test
  public void testIntermediateRecord() {
    DataSchema dataSchema = null;
    List<AggregationInfo> aggregationInfos = null;
    List<SelectionSort> selectionSort = null;

    OrderedIndexedTableResizer orderedResizer =
        new OrderedIndexedTableResizer(dataSchema, aggregationInfos, selectionSort);

    Record record = null;
    IntermediateRecord intermediateRecord = null;

    intermediateRecord = orderedResizer.getIntermediateRecord(record);

    // check conversion from Record to IntermediateRecord is correct,
    // for combinations of selection sorts and aggregation functions with non-comparable intermediate records
  }

  @Test
  public void testRandomResize() {
    IndexedTableResizer randomResizer = new RandomIndexedTableResizer();

    Map<Key, Record> recordsMap = null;
    int trimToSize = 0;
    randomResizer.resizeRecordsMap(recordsMap, trimToSize);

    // verify length after resize
  }
}

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
package org.apache.pinot.core.segment.processing.utils;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;


public final class SegmentProcessingUtils {
  private SegmentProcessingUtils() {
  }

  /**
   * Returns the field specs (physical only) with the names sorted in alphabetical order.
   */
  public static List<FieldSpec> getFieldSpecs(Schema schema) {
    List<FieldSpec> fieldSpecs = new ArrayList<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn()) {
        fieldSpecs.add(fieldSpec);
      }
    }
    fieldSpecs.sort(Comparator.comparing(FieldSpec::getName));
    return fieldSpecs;
  }

  /**
   * Returns the field specs (physical only) with sorted column in the front, followed by other columns sorted in
   * alphabetical order.
   */
  public static List<FieldSpec> getFieldSpecs(Schema schema, List<String> sortOrder) {
    List<FieldSpec> fieldSpecs = new ArrayList<>();
    for (String sortColumn : sortOrder) {
      FieldSpec fieldSpec = schema.getFieldSpecFor(sortColumn);
      Preconditions.checkArgument(fieldSpec != null, "Failed to find sort column: %s", sortColumn);
      Preconditions.checkArgument(fieldSpec.isSingleValueField(), "Cannot sort on MV column: %s", sortColumn);
      Preconditions.checkArgument(!fieldSpec.isVirtualColumn(), "Cannot sort on virtual column: %s", sortColumn);
      fieldSpecs.add(fieldSpec);
    }

    List<FieldSpec> nonSortFieldSpecs = new ArrayList<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn() && !sortOrder.contains(fieldSpec.getName())) {
        nonSortFieldSpecs.add(fieldSpec);
      }
    }
    nonSortFieldSpecs.sort(Comparator.comparing(FieldSpec::getName));

    fieldSpecs.addAll(nonSortFieldSpecs);
    return fieldSpecs;
  }

  /**
   * Returns the value comparator based on the sort order.
   */
  public static SortOrderComparator getSortOrderComparator(List<FieldSpec> fieldSpecs, int numSortColumns) {
    DataType[] sortColumnStoredTypes = new DataType[numSortColumns];
    for (int i = 0; i < numSortColumns; i++) {
      sortColumnStoredTypes[i] = fieldSpecs.get(i).getDataType().getStoredType();
    }
    return new SortOrderComparator(numSortColumns, sortColumnStoredTypes);
  }
}

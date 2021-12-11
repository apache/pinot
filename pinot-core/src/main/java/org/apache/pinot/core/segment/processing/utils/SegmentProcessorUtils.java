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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.core.segment.processing.framework.MergeType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public final class SegmentProcessorUtils {
  private SegmentProcessorUtils() {
  }

  /**
   * Returns the field specs (physical only) and number of sort fields based on the merge type and sort order.
   * <p>The field specs returned should have sorted columns in the front, followed by dimension/time columns sorted in
   * alphabetical order, followed by metric columns sorted in alphabetical order.
   * <p>For CONCAT, only include sort columns as sort fields;
   * <p>For ROLLUP, include sort columns and dimension/time columns as sort fields;
   * <p>For DEDUP, include all columns as sort fields.
   */
  public static Pair<List<FieldSpec>, Integer> getFieldSpecs(Schema schema, MergeType mergeType,
      @Nullable List<String> sortOrder) {
    if (sortOrder == null) {
      sortOrder = Collections.emptyList();
    }

    List<FieldSpec> fieldSpecs = new ArrayList<>();
    for (String sortColumn : sortOrder) {
      FieldSpec fieldSpec = schema.getFieldSpecFor(sortColumn);
      Preconditions.checkArgument(fieldSpec != null, "Failed to find sort column: %s", sortColumn);
      Preconditions.checkArgument(fieldSpec.isSingleValueField(), "Cannot sort on MV column: %s", sortColumn);
      Preconditions.checkArgument(!fieldSpec.isVirtualColumn(), "Cannot sort on virtual column: %s", sortColumn);
      Preconditions
          .checkArgument(fieldSpec.getFieldType() != FieldSpec.FieldType.METRIC || mergeType != MergeType.ROLLUP,
              "For ROLLUP, cannot sort on metric column: %s", sortColumn);
      fieldSpecs.add(fieldSpec);
    }

    List<FieldSpec> metricFieldSpecs = new ArrayList<>();
    List<FieldSpec> nonMetricFieldSpecs = new ArrayList<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn() && !sortOrder.contains(fieldSpec.getName())) {
        if (fieldSpec.getFieldType() == FieldSpec.FieldType.METRIC) {
          metricFieldSpecs.add(fieldSpec);
        } else {
          nonMetricFieldSpecs.add(fieldSpec);
        }
      }
    }

    metricFieldSpecs.sort(Comparator.comparing(FieldSpec::getName));
    fieldSpecs.addAll(nonMetricFieldSpecs);
    nonMetricFieldSpecs.sort(Comparator.comparing(FieldSpec::getName));
    fieldSpecs.addAll(metricFieldSpecs);

    int numSortFields;
    switch (mergeType) {
      case CONCAT:
        numSortFields = sortOrder.size();
        break;
      case ROLLUP:
        numSortFields = sortOrder.size() + nonMetricFieldSpecs.size();
        break;
      case DEDUP:
        numSortFields = fieldSpecs.size();
        break;
      default:
        throw new IllegalStateException("Unsupported merge type: " + mergeType);
    }

    return new ImmutablePair<>(fieldSpecs, numSortFields);
  }
}

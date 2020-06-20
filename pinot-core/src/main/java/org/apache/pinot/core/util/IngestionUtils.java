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
package org.apache.pinot.core.util;

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.core.data.function.FunctionEvaluator;
import org.apache.pinot.core.data.function.FunctionEvaluatorFactory;
import org.apache.pinot.spi.config.table.IngestionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Utility methods for extracting source and destination fields from ingestion configs
 */
public class IngestionUtils {

  /**
   * Extracts all fields required by the {@link org.apache.pinot.spi.data.readers.RecordExtractor} from the given TableConfig and Schema
   */
  public static Set<String> getFieldsForRecordExtractor(TableConfig tableConfig, Schema schema) {
    Set<String> fieldsForRecordExtractor = new HashSet<>();
    fieldsForRecordExtractor.addAll(getFieldsForRecordExtractor(tableConfig.getIngestionConfig()));
    fieldsForRecordExtractor.addAll(getFieldsForRecordExtractor(schema));
    return fieldsForRecordExtractor;
  }

  /**
   * Extracts all the fields needed by the {@link org.apache.pinot.spi.data.readers.RecordExtractor} from the given Schema
   * TODO: for now, we assume that arguments to transform function are in the source i.e. no columns are derived from transformed columns
   */
  private static Set<String> getFieldsForRecordExtractor(Schema schema) {
    Set<String> fieldNames = new HashSet<>();

    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn()) {
        FunctionEvaluator functionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(fieldSpec);
        if (functionEvaluator != null) {
          fieldNames.addAll(functionEvaluator.getArguments());
        }
        fieldNames.add(fieldSpec.getName());
      }
    }
    return fieldNames;
  }

  /**
   * Extracts the fields needed by a RecordExtractor from given {@link IngestionConfig}
   */
  private static Set<String> getFieldsForRecordExtractor(@Nullable IngestionConfig ingestionConfig) {
    if (ingestionConfig != null && ingestionConfig.getFilterConfig() != null) {
      String filterFunction = ingestionConfig.getFilterConfig().getFilterFunction();
      if (filterFunction != null) {
        FunctionEvaluator functionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(filterFunction);
        if (functionEvaluator != null) {
          return Sets.newHashSet(functionEvaluator.getArguments());
        }
      }
    }
    return Collections.emptySet();
  }

  /**
   * Returns true if the record doesn not contain key {@link GenericRow#FILTER_RECORD_KEY} with value true
   */
  public static boolean passedFilter(GenericRow genericRow) {
    return !Boolean.TRUE.equals(genericRow.getValue(GenericRow.FILTER_RECORD_KEY));
  }
}

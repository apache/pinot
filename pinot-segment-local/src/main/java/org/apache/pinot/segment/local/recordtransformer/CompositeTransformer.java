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
package org.apache.pinot.segment.local.recordtransformer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * The {@code CompositeTransformer} class performs multiple transforms based on the inner {@link RecordTransformer}s.
 */
public class CompositeTransformer implements RecordTransformer {
  private final List<RecordTransformer> _transformers;

  /**
   * Returns a record transformer that performs null value handling, time/expression/data-type transformation and record
   * sanitization.
   * <p>NOTE: DO NOT CHANGE THE ORDER OF THE RECORD TRANSFORMERS
   * <ul>
   *   <li>
   *     Optional {@link ExpressionTransformer} before everyone else, so that we get the real columns for other
   *     transformers to work on
   *   </li>
   *   <li>
   *     Optional {@link FilterTransformer} after {@link ExpressionTransformer}, so that we have source as well as
   *     destination columns
   *   </li>
   *   <li>
   *     Optional {@link SchemaConformingTransformer} after {@link FilterTransformer}, so that we can transform input
   *     records that have varying fields to a fixed schema without dropping any fields
   *   </li>
   *   <li>
   *     {@link DataTypeTransformer} after {@link SchemaConformingTransformer} to convert values to comply with the
   *     schema
   *   </li>
   *   <li>
   *     Optional {@link TimeValidationTransformer} after {@link DataTypeTransformer} so that time value is converted to
   *     the correct type
   *   </li>
   *   <li>
   *     {@link NullValueTransformer} after {@link DataTypeTransformer} and {@link TimeValidationTransformer} because
   *     empty Collection/Map/Object[] and invalid values can be replaced with null
   *   </li>
   *   <li>
   *     Optional {@link SanitizationTransformer} after {@link NullValueTransformer} so that before sanitation, all
   *     values are non-null and follow the data types defined in the schema
   *   </li>
   *   <li>
   *     {@link SpecialValueTransformer} after {@link DataTypeTransformer} so that we already have the values complying
   *      with the schema before handling special values and before {@link NullValueTransformer} so that it transforms
   *      all the null values properly
   *   </li>
   * </ul>
   */
  public static List<RecordTransformer> getDefaultTransformers(TableConfig tableConfig, Schema schema) {
    return Stream.of(new ExpressionTransformer(tableConfig, schema), new FilterTransformer(tableConfig),
            new SchemaConformingTransformer(tableConfig, schema), new DataTypeTransformer(tableConfig, schema),
            new TimeValidationTransformer(tableConfig, schema), new SpecialValueTransformer(schema),
            new NullValueTransformer(tableConfig, schema), new SanitizationTransformer(schema)).filter(t -> !t.isNoOp())
        .collect(Collectors.toList());
  }

  public static CompositeTransformer getDefaultTransformer(TableConfig tableConfig, Schema schema) {
    return new CompositeTransformer(getDefaultTransformers(tableConfig, schema));
  }

  /**
   * Includes custom and default transformers.
   * @param customTransformers
   * @param tableConfig
   * @param schema
   * @return
   */
  public static CompositeTransformer composeAllTransformers(List<RecordTransformer> customTransformers,
      TableConfig tableConfig, Schema schema) {
    List<RecordTransformer> allTransformers = new ArrayList<>(customTransformers);
    allTransformers.addAll(getDefaultTransformers(tableConfig, schema));
    return new CompositeTransformer(allTransformers);
  }

  /**
   * Returns a pass through record transformer that does not transform the record.
   */
  public static CompositeTransformer getPassThroughTransformer() {
    return new CompositeTransformer(Collections.emptyList());
  }

  public CompositeTransformer(List<RecordTransformer> transformers) {
    _transformers = transformers;
  }

  @Nullable
  @Override
  public GenericRow transform(GenericRow record) {
    for (RecordTransformer transformer : _transformers) {
      record = transformer.transform(record);
      if (record == null) {
        return null;
      }
    }
    return record;
  }
}

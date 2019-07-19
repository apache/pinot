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
package org.apache.pinot.core.data.recordtransformer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;


/**
 * The {@code CompositeTransformer} class performs multiple transforms based on the inner {@link RecordTransformer}s.
 */
public class CompositeTransformer implements RecordTransformer {
  private final List<RecordTransformer> _transformers;

  /**
   * Returns a record transformer that performs time transform, expressions transform and data type transform.
   * <p>NOTE: DO NOT CHANGE THE ORDER OF THE RECORD TRANSFORMERS
   * <ul>
   *   <li>
   *     We put {@link ExpressionTransformer} after {@link TimeTransformer} so that expression can work on outgoing time
   *     column
   *   </li>
   *   <li>
   *     We put {@link SanitizationTransformer} after {@link DataTypeTransformer} so that before sanitation, all values
   *     follow the data types defined in the {@link Schema}.
   *   </li>
   * </ul>
   */
  public static CompositeTransformer getDefaultTransformer(Schema schema) {
    return new CompositeTransformer(Arrays
        .asList(new NullValueTransformer(schema), new TimeTransformer(schema), new ExpressionTransformer(schema), new DataTypeTransformer(schema),
            new SanitizationTransformer(schema)));
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

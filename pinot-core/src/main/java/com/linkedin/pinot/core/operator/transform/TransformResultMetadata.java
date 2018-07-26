/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.transform;

import com.linkedin.pinot.common.data.FieldSpec;


/**
 * The <code>TransformResultMetadata</code> class contains the metadata for the transform result.
 */
public class TransformResultMetadata {
  private final FieldSpec.DataType _dataType;
  private final boolean _isSingleValue;
  private final boolean _hasDictionary;

  public TransformResultMetadata(FieldSpec.DataType dataType, boolean isSingleValue, boolean hasDictionary) {
    _dataType = dataType;
    _isSingleValue = isSingleValue;
    _hasDictionary = hasDictionary;
  }

  public FieldSpec.DataType getDataType() {
    return _dataType;
  }

  public boolean isSingleValue() {
    return _isSingleValue;
  }

  public boolean hasDictionary() {
    return _hasDictionary;
  }
}

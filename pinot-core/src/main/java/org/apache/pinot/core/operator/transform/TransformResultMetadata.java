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
package org.apache.pinot.core.operator.transform;

import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * The <code>TransformResultMetadata</code> class contains the metadata for the transform result.
 */
public class TransformResultMetadata {
  private final DataType _dataType;
  private final boolean _isSingleValue;
  private final boolean _hasDictionary;

  public TransformResultMetadata(DataType dataType, boolean isSingleValue, boolean hasDictionary) {
    _dataType = dataType;
    _isSingleValue = isSingleValue;
    _hasDictionary = hasDictionary;
  }

  public DataType getDataType() {
    return _dataType;
  }

  public boolean isSingleValue() {
    return _isSingleValue;
  }

  public boolean hasDictionary() {
    return _hasDictionary;
  }
}

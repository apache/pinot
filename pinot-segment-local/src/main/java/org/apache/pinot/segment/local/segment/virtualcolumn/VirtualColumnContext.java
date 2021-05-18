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
package org.apache.pinot.segment.local.segment.virtualcolumn;

import org.apache.pinot.spi.data.FieldSpec;


/**
 * Miscellaneous context information about the column.
 * It will be used to build various components (dictionary, reader, etc) in the virtual column provider.
 */
public class VirtualColumnContext {
  private FieldSpec _fieldSpec;
  private int _totalDocCount;

  public VirtualColumnContext(FieldSpec fieldSpec, int totalDocCount) {
    _fieldSpec = fieldSpec;
    _totalDocCount = totalDocCount;
  }

  public FieldSpec getFieldSpec() {
    return _fieldSpec;
  }

  public int getTotalDocCount() {
    return _totalDocCount;
  }
}

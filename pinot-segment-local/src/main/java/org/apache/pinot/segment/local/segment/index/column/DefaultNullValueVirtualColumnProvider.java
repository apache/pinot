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
package org.apache.pinot.segment.local.segment.index.column;

import org.apache.pinot.segment.local.segment.virtualcolumn.BaseConstantValueVirtualColumnProvider;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnContext;


/// Provides the column's default null value for every document.
///
/// This is also how `$hostName` and `$segmentName` are served: their value is constant for the segment and is carried
/// as the field's default null value.
///
/// NOTE: The fully-qualified name of this class is stored in field specs (see
/// `DimensionFieldSpec#DimensionFieldSpec(String, DataType, boolean, Class)`) and resolved reflectively, so it must
/// not be moved or renamed.
public class DefaultNullValueVirtualColumnProvider extends BaseConstantValueVirtualColumnProvider {

  @Override
  protected Object getValue(VirtualColumnContext context) {
    return context.getFieldSpec().getDefaultNullValue();
  }
}

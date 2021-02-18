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

package org.apache.pinot.controller.recommender.io.metadata;

import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.Schema;


/**
 * This class is used in {@link SchemaWithMetaData} to add metadata to {@link DateTimeFieldSpec}. Without this object,
 * json representation of {@link SchemaWithMetaData} object cannot be deserialized to {@link Schema} object.
 */
public class DateTimeFieldSpecMetadata extends FieldMetadata {
  private String _format;
  private String _granularity;

  public String getFormat() {
    return _format;
  }

  public void setFormat(String format) {
    _format = format;
  }

  public String getGranularity() {
    return _granularity;
  }

  public void setGranularity(String granularity) {
    _granularity = granularity;
  }

  @Override
  public FieldType getFieldType() {
    return FieldType.DATE_TIME;
  }
}

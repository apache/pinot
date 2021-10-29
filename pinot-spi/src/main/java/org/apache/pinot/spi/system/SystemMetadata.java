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
package org.apache.pinot.spi.system;

import org.apache.pinot.spi.data.FieldSpec;


/**
 * Metadata interface that defines the schema of the metadata, e.g. the dimensions and metrics.
 */
public interface SystemMetadata {

  /**
   * get column name of the system metadata.
   * @return the schema of the system metadata.
   */
  String[] getColumnName();

  /**
   * get column data type of the system metadata.
   * @return the schema of the system metadata.
   */
  FieldSpec.DataType[] getColumnDataType();

  /**
   * name of the system metadata, used to identify the type of metadata contents.
   *
   * @return system metadata name.
   */
  String getSystemMetadataName();
}

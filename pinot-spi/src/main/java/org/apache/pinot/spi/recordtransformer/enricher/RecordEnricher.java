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
package org.apache.pinot.spi.recordtransformer.enricher;

import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;


/**
 * Record enricher is a special {@link RecordTransformer} which is applied before other transformers to enrich the
 * columns. If a column with the same name as the input column already exists in the record, it will be overwritten.
 */
public interface RecordEnricher extends RecordTransformer {

  /**
   * Enriches the given record, by adding new columns to the same record.
   */
  void enrich(GenericRow record);

  @Override
  default GenericRow transform(GenericRow record) {
    enrich(record);
    return record;
  }
}

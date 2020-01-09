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
package org.apache.pinot.plugin.inputformat.avro;

import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;

public class AvroRecordExtractor extends RecordExtractor<GenericData.Record> {
  @Override
  public GenericRow extract(Schema schema, GenericData.Record from, GenericRow to) {
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      FieldSpec incomingFieldSpec = getFieldSpecToUse(schema, fieldSpec);
      AvroUtils.extractField(incomingFieldSpec, from, to);
    }
    return to;
  }
}

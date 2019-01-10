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

package com.linkedin.thirdeye.hadoop.transform;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTransformUDF implements TransformUDF {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTransformUDF.class);

  private Schema outputSchema;

  public DefaultTransformUDF() {

  }

  @Override
  public void init(Schema outputSchema) {
    this.outputSchema = outputSchema;
  }

  @Override
  public GenericRecord transformRecord(String sourceName, GenericRecord record) {
    // Default implementation returns input record as is
    return record;
  }

}

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
package org.apache.pinot.common.utils.config;

import java.io.IOException;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.data.Schema;


/// Util class for serializing and deserializing [Schema] to and from [ZNRecord].
public class SchemaSerDeUtils {
  private SchemaSerDeUtils() {
  }

  /**
   * Fetch {@link Schema} from a {@link ZNRecord}.
   */
  public static Schema fromZNRecord(ZNRecord record)
      throws IOException {
    String schemaJSON = record.getSimpleField("schemaJSON");
    return Schema.fromString(schemaJSON);
  }

  /**
   * Wrap {@link Schema} into a {@link ZNRecord}.
   */
  public static ZNRecord toZNRecord(Schema schema) {
    ZNRecord record = new ZNRecord(schema.getSchemaName());
    record.setSimpleField("schemaJSON", schema.toSingleLineJsonString());
    return record;
  }
}

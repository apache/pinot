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
package org.apache.pinot.segment.local.utils.migration;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.migration.SchemaMigrator;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.JsonUtils;


/// Replaces legacy TIME fields with equivalent DATE_TIME fields, retaining their common field attributes.
/// The existing schema conversion supplies the time format, granularity, and any required ingestion transform.
/// This stateless migrator is safe to invoke concurrently on distinct schema copies.
// Legacy TIME and transform APIs are intentionally read to migrate existing stored schemas.
@SuppressWarnings("deprecation")
public class LegacyTimeFieldSchemaMigrator implements SchemaMigrator {
  @Override
  public int fromVersion() {
    return 0;
  }

  @Override
  public Schema migrate(Schema input) {
    TimeFieldSpec timeFieldSpec = input.getTimeFieldSpec();
    if (timeFieldSpec == null) {
      return input;
    }
    TimeGranularitySpec incoming = timeFieldSpec.getIncomingGranularitySpec();
    TimeGranularitySpec outgoing = timeFieldSpec.getOutgoingGranularitySpec();
    // An empty transform still selects the legacy time converter, but the helper would copy it as an identity.
    Preconditions.checkState(!"".equals(timeFieldSpec.getTransformFunction()) || incoming.equals(outgoing),
        "Cannot automatically migrate time field with empty conversion transform: %s", timeFieldSpec.getName());
    boolean nonUnitMillis = incoming.getTimeType() == TimeUnit.MILLISECONDS && incoming.getTimeUnitSize() != 1
        || outgoing.getTimeType() == TimeUnit.MILLISECONDS && outgoing.getTimeUnitSize() != 1;
    // The conversion helper does not multiply/divide non-unit millisecond values when generating a transform.
    Preconditions.checkState(
        !nonUnitMillis || incoming.equals(outgoing) || timeFieldSpec.getTransformFunction() != null,
        "Cannot automatically migrate time field with non-unit millisecond conversion: %s", timeFieldSpec.getName());
    DateTimeFieldSpec converted = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    // Preserve all common attributes, including column identity and nullability, which the conversion helper
    // predates. Replace only the TIME representation and the converted format/granularity/transform.
    ObjectNode attributes = (ObjectNode) JsonUtils.objectToJsonNode(timeFieldSpec);
    attributes.remove(List.of("incomingGranularitySpec", "outgoingGranularitySpec", "name", "dataType", "fieldType",
        "transformFunction"));
    ObjectNode field = converted.toJsonObject();
    field.setAll(attributes);
    try {
      converted = JsonUtils.jsonNodeToObject(field, DateTimeFieldSpec.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Cannot convert legacy time field: " + timeFieldSpec.getName(), e);
    }
    input.removeField(timeFieldSpec.getName());
    input.addField(converted);
    return input;
  }
}

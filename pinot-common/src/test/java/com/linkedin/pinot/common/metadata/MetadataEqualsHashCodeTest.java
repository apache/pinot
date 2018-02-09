/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.metadata;

import com.linkedin.pinot.common.data.DateTimeFieldSpec;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;

import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;


/**
 * Test for equals and hashCode for the various ZK metadata classes
 *
 */
public class MetadataEqualsHashCodeTest {
  @Test
  public void testEqualsAndHashCode() {
    EqualsVerifier.forClass(InstanceZKMetadata.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS).
        usingGetClass().verify();

    EqualsVerifier.forClass(SegmentZKMetadata.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS).
        usingGetClass().verify();
    EqualsVerifier.forClass(OfflineSegmentZKMetadata.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS).
        usingGetClass().verify();
    EqualsVerifier.forClass(RealtimeSegmentZKMetadata.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS).
        usingGetClass().verify();

    EqualsVerifier.forClass(KafkaStreamMetadata.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS).
        usingGetClass().verify();

    EqualsVerifier.forClass(Schema.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS).
        usingGetClass().verify();
    EqualsVerifier.forClass(DimensionFieldSpec.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS, Warning.TRANSIENT_FIELDS).
        usingGetClass().verify();
    EqualsVerifier.forClass(MetricFieldSpec.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS, Warning.TRANSIENT_FIELDS).
        usingGetClass().verify();
    EqualsVerifier.forClass(TimeFieldSpec.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS, Warning.TRANSIENT_FIELDS).
        usingGetClass().verify();
    EqualsVerifier.forClass(DateTimeFieldSpec.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS, Warning.TRANSIENT_FIELDS).
        usingGetClass().verify();
  }
}

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
package org.apache.pinot.common.metadata;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.testng.annotations.Test;


/**
 * Test for equals and hashCode for the various ZK metadata classes
 *
 */
public class MetadataEqualsHashCodeTest {

  @Test
  public void testEqualsAndHashCode() {
    EqualsVerifier.forClass(InstanceZKMetadata.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS)
        .usingGetClass().verify();
    EqualsVerifier.forClass(DimensionFieldSpec.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS)
        .usingGetClass().verify();
    EqualsVerifier.forClass(MetricFieldSpec.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS)
        .usingGetClass().verify();
    EqualsVerifier.forClass(TimeFieldSpec.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS).usingGetClass()
        .verify();
    EqualsVerifier.forClass(DateTimeFieldSpec.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS)
        .usingGetClass().verify();
    // NOTE: Suppress Warning.ALL_FIELDS_SHOULD_BE_USED because some fields are derived from other fields, but we cannot
    //       declare them as transitive because they are still needed after ser/de
    EqualsVerifier.forClass(Schema.class)
        .suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS, Warning.ALL_FIELDS_SHOULD_BE_USED).usingGetClass()
        .verify();
  }
}

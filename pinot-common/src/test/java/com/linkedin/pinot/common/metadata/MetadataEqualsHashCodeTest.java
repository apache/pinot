/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.DataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.OfflineDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.RealtimeDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.testng.annotations.Test;


/**
 * TODO Document me!
 *
 * @author jfim
 */
public class MetadataEqualsHashCodeTest {
  @Test
  public void testEqualsAndHashCode() {
    EqualsVerifier.forClass(InstanceZKMetadata.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS).
        usingGetClass().verify();

    EqualsVerifier.forClass(DataResourceZKMetadata.class).
        suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS).usingGetClass().verify();
    EqualsVerifier.forClass(OfflineDataResourceZKMetadata.class).
        suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS).usingGetClass().verify();
    EqualsVerifier.forClass(RealtimeDataResourceZKMetadata.class).
        suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS).usingGetClass().verify();

    EqualsVerifier.forClass(SegmentZKMetadata.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS).
        usingGetClass().verify();
    EqualsVerifier.forClass(OfflineSegmentZKMetadata.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS).
        usingGetClass().verify();
    EqualsVerifier.forClass(RealtimeSegmentZKMetadata.class).suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS).
        usingGetClass().verify();
  }
}

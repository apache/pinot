/*
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
package org.apache.pinot.common.data.schema;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;

import static org.apache.pinot.common.data.schema.SchemaMetadataConstants.DEFAULT_CARDINALITY;
import static org.apache.pinot.common.data.schema.SchemaMetadataConstants.DEFAULT_DATA_LENGTH;


public class TimeFieldSpecMetadata extends FieldMetadata {

  private TimeGranularitySpecMetadata _incomingGranularitySpec;
  private TimeGranularitySpecMetadata _outgoingGranularitySpec;

  public TimeGranularitySpecMetadata getIncomingGranularitySpec() {
    return _incomingGranularitySpec;
  }

  public void setIncomingGranularitySpec(TimeGranularitySpecMetadata incomingGranularitySpec) {
    _incomingGranularitySpec = incomingGranularitySpec;
    if (_outgoingGranularitySpec == null) {
      super.setNumValuesPerEntry(DEFAULT_CARDINALITY);
      super.setAverageLength(DEFAULT_DATA_LENGTH);
      super.setCardinality(incomingGranularitySpec.getCardinality());
      super.setName(incomingGranularitySpec.getName());
      super.setDataType(incomingGranularitySpec.getDataType());
    }
  }

  public TimeGranularitySpecMetadata getOutgoingGranularitySpec() {
    return _outgoingGranularitySpec;
  }

  public void setOutgoingGranularitySpec(TimeGranularitySpecMetadata outgoingGranularitySpec) {
    _outgoingGranularitySpec = outgoingGranularitySpec;
    super.setNumValuesPerEntry(DEFAULT_CARDINALITY);
    super.setAverageLength(DEFAULT_DATA_LENGTH);
    super.setCardinality(outgoingGranularitySpec.getCardinality());
    super.setName(outgoingGranularitySpec.getName());
    super.setDataType(outgoingGranularitySpec.getDataType());
  }

  @Override
  @JsonSetter(nulls = Nulls.SKIP)
  public void setAverageLength(int averageLength) {
    ;
  }

  @Override
  @JsonSetter(nulls = Nulls.SKIP)
  public void setNumValuesPerEntry(double numValuesPerEntry) {
    ;
  }

  @Override
  @JsonSetter(nulls = Nulls.SKIP)
  public void setCardinality(int cardinality) {
    ;
  }

  @Override
  public FieldType getFieldType() {
    return FieldType.DATE_TIME;
  }
}

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
package org.apache.pinot.controller.recommender.io.metadata;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;


public class TimeFieldSpecMetadata extends FieldMetadata {

  private FieldMetadata _incomingGranularitySpec;
  private FieldMetadata _outgoingGranularitySpec;

  public FieldMetadata getIncomingGranularitySpec() {
    return _incomingGranularitySpec;
  }

  public void setIncomingGranularitySpec(FieldMetadata incomingGranularitySpec) {
    _incomingGranularitySpec = incomingGranularitySpec;
    if (_outgoingGranularitySpec == null) {
      super.setNumValuesPerEntry(incomingGranularitySpec.getNumValuesPerEntry());
      super.setAverageLength(incomingGranularitySpec.getAverageLength());
      super.setCardinality(incomingGranularitySpec.getCardinality());
      super.setName(incomingGranularitySpec.getName());
    }
  }

  public FieldMetadata getOutgoingGranularitySpec() {
    return _outgoingGranularitySpec;
  }

  public void setOutgoingGranularitySpec(FieldMetadata outgoingGranularitySpec) {
    _outgoingGranularitySpec = outgoingGranularitySpec;
    super.setNumValuesPerEntry(outgoingGranularitySpec.getNumValuesPerEntry());
    super.setAverageLength(outgoingGranularitySpec.getAverageLength());
    super.setCardinality(outgoingGranularitySpec.getCardinality());
    super.setName(outgoingGranularitySpec.getName());
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
  public void setCardinality(double cardinality) {
    ;
  }

  @Override
  public FieldType getFieldType() {
    return FieldType.DATE_TIME;
  }
}

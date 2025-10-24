package org.apache.pinot.spi.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class IntermediateFieldSpec extends FieldSpec {

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public IntermediateFieldSpec() {
    super();
  }

  @JsonIgnore
  @Override
  public FieldType getFieldType() {
    return FieldType.INTERMEDIATE;
  }
}

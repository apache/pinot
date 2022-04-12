package org.apache.pinot.spi.utils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public class TestClass {
  int primitiveIntegerField;
  String stringField;
  Long longField;
  Klass classField;

  public int getPrimitiveIntegerField() {
    return primitiveIntegerField;
  }

  public void setPrimitiveIntegerField(int primitiveIntegerField) {
    this.primitiveIntegerField = primitiveIntegerField;
  }

  public String getStringField() {
    return stringField;
  }

  public void setStringField(String stringField) {
    this.stringField = stringField;
  }

  public Long getLongField() {
    return longField;
  }

  public void setLongField(Long longField) {
    this.longField = longField;
  }

  public Klass getClassField() {
    return classField;
  }

  public void setClassField(Klass classField) {
    this.classField = classField;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public class Klass {
    Integer internalIntField;
    String internalStringField;

    public int getInternalIntField() {
      return internalIntField;
    }

    public void setInternalIntField(int internalIntField) {
      this.internalIntField = internalIntField;
    }

    public String getInternalStringField() {
      return internalStringField;
    }

    public void setInternalStringField(String internalStringField) {
      this.internalStringField = internalStringField;
    }
  }
}

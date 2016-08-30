package com.linkedin.thirdeye.hadoop.join;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

public class GenericJoinUDFConfig {

  List<Field> fields;

  public GenericJoinUDFConfig(Map<String, String> params) {
    fields = new ArrayList<Field>();
    String fieldNamesString = params.get("field.names");
    String[] split = fieldNamesString.split(",");
    for (String fieldName : split) {
      Field field = new Field();
      field.name = fieldName;
      String type = params.get(fieldName + ".type");
      if (type != null) {
        field.type = Schema.Type.valueOf(type.toUpperCase());
      }
      field.sourceEvents = new ArrayList<String>();
      String[] fieldSources = params.get(fieldName + ".sources").split(",");
      for (String fieldSource : fieldSources) {
        field.sourceEvents.add(fieldSource.trim());
      }
      fields.add(field);
    }
  }

  public List<Field> getFields() {
    return fields;
  }

  public void setFields(List<Field> fields) {
    this.fields = fields;
  }

  /*
   * For now support name and source Name. Will be nice to support data type
   * conversion and transform function in future
   */
  public static class Field {
    String name;
    List<String> sourceEvents;
    Schema.Type type;
    List<String> tranformFunc;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Type getType() {
      return type;
    }

    public void setType(Type type) {
      this.type = type;
    }

    public List<String> getSourceEvents() {
      return sourceEvents;
    }

    public void setSourceEvents(List<String> sourceEvents) {
      this.sourceEvents = sourceEvents;
    }

    public List<String> getTranformFunc() {
      return tranformFunc;
    }

    public void setTranformFunc(List<String> tranformFunc) {
      this.tranformFunc = tranformFunc;
    }
  }
}

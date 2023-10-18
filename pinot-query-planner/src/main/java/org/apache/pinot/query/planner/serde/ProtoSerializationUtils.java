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
package org.apache.pinot.query.planner.serde;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Utils to convert automatically from/to object that's implementing {@link ProtoSerializable}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ProtoSerializationUtils {
  private static final String ENUM_VALUE_KEY = "ENUM_VALUE_KEY";
  private static final String NULL_OBJECT_CLASSNAME = "null";
  private static final Plan.ObjectField NULL_OBJECT_VALUE = Plan.ObjectField.newBuilder()
      .setObjectClassName(NULL_OBJECT_CLASSNAME).build();

  private ProtoSerializationUtils() {
    // do not instantiate.
  }

  /**
   * Reflectively set object's field based on {@link Plan.ObjectField} provided.
   *
   * @param object the object to be set.
   * @param objectField the proto ObjectField from which the object will be set.
   */
  public static void setObjectFieldToObject(Object object, Plan.ObjectField objectField) {
    Map<String, Plan.MemberVariableField> memberVariablesMap = objectField.getMemberVariablesMap();
    for (Map.Entry<String, Plan.MemberVariableField> e : memberVariablesMap.entrySet()) {
      try {
        Field declaredField = object.getClass().getDeclaredField(e.getKey());
        if (declaredField.isAnnotationPresent(ProtoProperties.class)) {
          Object memberVarObject = constructMemberVariable(e.getValue());
          if (memberVarObject != null) {
            declaredField.setAccessible(true);
            declaredField.set(object, memberVarObject);
          }
        }
      } catch (NoSuchFieldException | IllegalAccessException ex) {
        throw new IllegalStateException("Unable to set Object " + object.getClass() + " on field " + e.getKey()
            + "with object of type: " + objectField.getObjectClassName(), ex);
      }
    }
  }

  /**
   * Convert object into a proto {@link Plan.ObjectField}.
   *
   * @param object object to be converted.
   * @return the converted proto ObjectField.
   */
  public static Plan.ObjectField convertObjectToObjectField(Object object) {
    if (object != null) {
      Plan.ObjectField.Builder builder = Plan.ObjectField.newBuilder();
      builder.setObjectClassName(object.getClass().getName());
      // special handling for enum
      if (object instanceof Enum) {
        builder.putMemberVariables(ENUM_VALUE_KEY, serializeMemberVariable(((Enum) object).name()));
      } else {
        try {
          for (Field field : object.getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(ProtoProperties.class)) {
              field.setAccessible(true);
              Object fieldObject = field.get(object);
              builder.putMemberVariables(field.getName(), serializeMemberVariable(fieldObject));
            }
          }
        } catch (IllegalAccessException e) {
          throw new IllegalStateException("Unable to serialize Object: " + object.getClass(), e);
        }
      }
      return builder.build();
    } else {
      return NULL_OBJECT_VALUE;
    }
  }

  // --------------------------------------------------------------------------
  // Serialize Utils
  // --------------------------------------------------------------------------

  private static Plan.LiteralField boolField(boolean val) {
    return Plan.LiteralField.newBuilder().setBoolField(val).build();
  }

  private static Plan.LiteralField intField(int val) {
    return Plan.LiteralField.newBuilder().setIntField(val).build();
  }

  private static Plan.LiteralField longField(long val) {
    return Plan.LiteralField.newBuilder().setLongField(val).build();
  }

  private static Plan.LiteralField floatField(float val) {
    return Plan.LiteralField.newBuilder().setFloatField(val).build();
  }

  private static Plan.LiteralField doubleField(double val) {
    return Plan.LiteralField.newBuilder().setDoubleField(val).build();
  }

  private static Plan.LiteralField stringField(String val) {
    return Plan.LiteralField.newBuilder().setStringField(val).build();
  }

  private static Plan.LiteralField bytesField(ByteArray val) {
    return Plan.LiteralField.newBuilder().setBytesField(ByteString.copyFrom(val.getBytes())).build();
  }

  private static Plan.MemberVariableField serializeMemberVariable(Object fieldObject) {
    Plan.MemberVariableField.Builder builder = Plan.MemberVariableField.newBuilder();
    if (fieldObject instanceof Boolean) {
      builder.setLiteralField(boolField((Boolean) fieldObject));
    } else if (fieldObject instanceof Integer) {
      builder.setLiteralField(intField((Integer) fieldObject));
    } else if (fieldObject instanceof Long) {
      builder.setLiteralField(longField((Long) fieldObject));
    } else if (fieldObject instanceof Float) {
      builder.setLiteralField(floatField((Float) fieldObject));
    } else if (fieldObject instanceof Double) {
      builder.setLiteralField(doubleField((Double) fieldObject));
    } else if (fieldObject instanceof String) {
      builder.setLiteralField(stringField((String) fieldObject));
    } else if (fieldObject instanceof ByteArray) {
      builder.setLiteralField(bytesField((ByteArray) fieldObject));
    } else if (fieldObject instanceof List) {
      builder.setListField(serializeListMemberVariable(fieldObject));
    } else if (fieldObject instanceof Map) {
      builder.setMapField(serializeMapMemberVariable(fieldObject));
    } else {
      builder.setObjectField(convertObjectToObjectField(fieldObject));
    }
    return builder.build();
  }

  private static Plan.ListField serializeListMemberVariable(Object fieldObject) {
    Preconditions.checkState(fieldObject instanceof List);
    Plan.ListField.Builder builder = Plan.ListField.newBuilder();
    for (Object e : (List) fieldObject) {
      builder.addContent(serializeMemberVariable(e));
    }
    return builder.build();
  }

  private static Plan.MapField serializeMapMemberVariable(Object fieldObject) {
    Preconditions.checkState(fieldObject instanceof Map);
    Plan.MapField.Builder builder = Plan.MapField.newBuilder();
    Set<Map.Entry<String, Object>> entrySet = ((Map) fieldObject).entrySet();
    for (Map.Entry<String, Object> e : entrySet) {
      builder.putContent(e.getKey(), serializeMemberVariable(e.getValue()));
    }
    return builder.build();
  }

  // --------------------------------------------------------------------------
  // Deserialize Utils
  // --------------------------------------------------------------------------

  private static Object constructMemberVariable(Plan.MemberVariableField memberVariableField) {
    switch (memberVariableField.getMemberVariableFieldCase()) {
      case LITERALFIELD:
        return constructLiteral(memberVariableField.getLiteralField());
      case LISTFIELD:
        return constructList(memberVariableField.getListField());
      case MAPFIELD:
        return constructMap(memberVariableField.getMapField());
      case OBJECTFIELD:
        return constructObject(memberVariableField.getObjectField());
      case MEMBERVARIABLEFIELD_NOT_SET:
      default:
        return null;
    }
  }

  private static Object constructLiteral(Plan.LiteralField literalField) {
    switch (literalField.getLiteralFieldCase()) {
      case BOOLFIELD:
        return literalField.getBoolField();
      case INTFIELD:
        return literalField.getIntField();
      case LONGFIELD:
        return literalField.getLongField();
      case FLOATFIELD:
        return literalField.getFloatField();
      case DOUBLEFIELD:
        return literalField.getDoubleField();
      case STRINGFIELD:
        return literalField.getStringField();
      case BYTESFIELD:
        return new ByteArray(literalField.getBytesField().toByteArray());
      case LITERALFIELD_NOT_SET:
      default:
        return null;
    }
  }

  private static List constructList(Plan.ListField listField) {
    List list = new ArrayList();
    for (Plan.MemberVariableField e : listField.getContentList()) {
      list.add(constructMemberVariable(e));
    }
    return list;
  }

  private static Map constructMap(Plan.MapField mapField) {
    Map map = new HashMap();
    for (Map.Entry<String, Plan.MemberVariableField> e : mapField.getContentMap().entrySet()) {
      map.put(e.getKey(), constructMemberVariable(e.getValue()));
    }
    return map;
  }

  private static Object constructObject(Plan.ObjectField objectField) {
    if (!NULL_OBJECT_CLASSNAME.equals(objectField.getObjectClassName())) {
      try {
        Class<?> clazz = Class.forName(objectField.getObjectClassName());
        if (clazz.isEnum()) {
          return Enum.valueOf((Class<Enum>) clazz,
              objectField.getMemberVariablesOrDefault(ENUM_VALUE_KEY, null).getLiteralField().getStringField());
        } else {
          Object obj = clazz.newInstance();
          setObjectFieldToObject(obj, objectField);
          return obj;
        }
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        throw new IllegalStateException("Unable to create Object of type: " + objectField.getObjectClassName(), e);
      }
    } else {
      return null;
    }
  }
}

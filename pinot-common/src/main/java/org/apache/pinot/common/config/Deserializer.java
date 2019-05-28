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
package org.apache.pinot.common.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigIncludeContext;
import com.typesafe.config.ConfigIncluder;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigParseOptions;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.pinot.common.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Deserializer from configuration values to Java objects.
 */
public class Deserializer {
  private static final Logger LOGGER = LoggerFactory.getLogger(Deserializer.class);

  private static final java.util.Map<Tuple2<Class, Class>, Function<Object, Object>> typeConverters = new HashMap<>();

  private static final java.util.Set<Class> simpleTypes;

  static {
    // String -> primitives
    typeConverters.put(Tuple.of(String.class, byte.class), str -> {
      try {
        return Byte.parseByte((String) str);
      } catch (NumberFormatException e) {
        return null;
      }
    });

    typeConverters.put(Tuple.of(String.class, char.class), str -> {
      String s = (String) str;
      if (s.length() >= 1) {
        return s.charAt(0);
      } else {
        return null;
      }
    });

    typeConverters.put(Tuple.of(String.class, short.class), str -> {
      try {
        return Short.parseShort((String) str);
      } catch (NumberFormatException e) {
        return null;
      }
    });

    typeConverters.put(Tuple.of(String.class, int.class), str -> {
      try {
        return Integer.parseInt((String) str);
      } catch (NumberFormatException e) {
        return null;
      }
    });

    typeConverters.put(Tuple.of(String.class, long.class), str -> {
      try {
        return Long.parseLong((String) str);
      } catch (NumberFormatException e) {
        return null;
      }
    });

    typeConverters.put(Tuple.of(String.class, float.class), str -> {
      try {
        return Float.parseFloat((String) str);
      } catch (NumberFormatException e) {
        return null;
      }
    });

    typeConverters.put(Tuple.of(String.class, double.class), str -> {
      try {
        return Double.parseDouble((String) str);
      } catch (NumberFormatException e) {
        return null;
      }
    });

    // Numbers -> primitives
    typeConverters.put(Tuple.of(Number.class, byte.class), num -> {
      Number n = (Number) num;
      return n.byteValue();
    });

    typeConverters.put(Tuple.of(Number.class, char.class), num -> {
      // jfim: This one is somewhat ambiguous, does foo=0 map to '0' or 0x00?
      return num.toString().charAt(0);
    });

    typeConverters.put(Tuple.of(Number.class, short.class), num -> {
      Number n = (Number) num;
      return n.byteValue();
    });

    typeConverters.put(Tuple.of(Number.class, int.class), num -> {
      Number n = (Number) num;
      return n.byteValue();
    });

    typeConverters.put(Tuple.of(Number.class, long.class), num -> {
      Number n = (Number) num;
      return n.byteValue();
    });

    typeConverters.put(Tuple.of(Number.class, float.class), num -> {
      Number n = (Number) num;
      return n.byteValue();
    });

    typeConverters.put(Tuple.of(Number.class, double.class), num -> {
      Number n = (Number) num;
      return n.byteValue();
    });

    typeConverters.put(Tuple.of(String[].class, Set.class), items -> {
      Set set = new HashSet<String>();
      String[] itemArray = (String[]) items;
      set.addAll(Arrays.asList(itemArray));
      return set;
    });

    typeConverters.put(Tuple.of(String[].class, java.util.List.class), items -> {
      java.util.List list = new ArrayList<String>();
      String[] itemArray = (String[]) items;
      list.addAll(Arrays.asList(itemArray));
      return list;
    });

    typeConverters.put(Tuple.of(ArrayList.class, Set.class), items -> {
      ArrayList list = (ArrayList) items;
      return new HashSet(list);
    });

    // Mark all types that have a converter as simple types
    simpleTypes = new HashSet<>();
    typeConverters.keySet().forEach(t2 -> simpleTypes.add(t2._2));
  }

  public static <T> T deserialize(Class<T> clazz, Map<String, ?> config, String configPath)
      throws Exception {
    LOGGER.debug("Deserializing object of class {} at config path {} using config {}", clazz.getName(), configPath,
        config);

    if (config == null) {
      LOGGER.debug("Config is null, returning null value");
      return null;
    }

    // Instantiate the root object
    T rootObject = clazz.newInstance();

    // Child key transformation?
    UseChildKeyTransformers useChildKeyTransformers = clazz.getAnnotation(UseChildKeyTransformers.class);
    if (useChildKeyTransformers != null) {
      for (Class<? extends ChildKeyTransformer> childKeyTransformerClass : useChildKeyTransformers.value()) {
        LOGGER.debug("Using child key transformer {} on the root config {}", childKeyTransformerClass, config);

        ChildKeyTransformer childKeyTransformer = childKeyTransformerClass.newInstance();
        config = childKeyTransformer.apply(config, configPath);

        LOGGER.debug("Config after child key transformation {}", config);
      }
    }

    // Need to subset the config?
    ConfigKey rootConfigKey = clazz.getAnnotation(ConfigKey.class);

    if (rootConfigKey != null) {
      String suffix = rootConfigKey.value() + ".";
      config = subset(suffix, config);
      configPath = configPath + suffix;

      LOGGER.debug("Using subset config {} and config path {}", config, configPath);
    }

    // Pre-inject hook?
    if (ConfigNodeLifecycleAware.class.isAssignableFrom(clazz)) {
      ((ConfigNodeLifecycleAware) rootObject).preInject();
    }

    // Iterate over the root class fields
    List<Field> declaredFields = getClassFields(clazz);

    boolean valueInjected = false;
    for (Field declaredField : declaredFields) {
      try {
        LOGGER.debug("Processing field {}", declaredField.getName());

        ConfigKey fieldConfigKey = declaredField.getAnnotation(ConfigKey.class);
        boolean isKeylessConfigField = declaredField.getAnnotation(NestedConfig.class) != null;
        UseChildKeyHandler useChildKeyHandler = declaredField.getAnnotation(UseChildKeyHandler.class);

        if (fieldConfigKey != null) {
          UseDsl dslInfo = declaredField.getAnnotation(UseDsl.class);

          final String keyName = fieldConfigKey.value();
          if (dslInfo != null) {
            // Use this DSL to parse the value
            LOGGER.debug("Using DSL {} to extract value {}", dslInfo.dsl(), dslInfo.value());
            SingleKeyDsl dsl = dslInfo.dsl().newInstance();
            if (config.containsKey(keyName)) {
              Object dslValue = dsl.parse(config.getOrElse(keyName, null).toString());
              Map<String, ?> dslValues = Serializer.serialize(dslValue);

              LOGGER.debug("Extracting value from field {} of {}, values are {}", dslInfo.value(), dslValue, dslValues);

              if (dslValues != null) {
                valueInjected |=
                    coerceValueIntoField(rootObject, declaredField, dslValues.getOrElse(dslInfo.value(), null));
              }
            }
          } else if (useChildKeyHandler != null) {
            // Use a child key handler to handle this value
            LOGGER.debug("Using child key handler {}", useChildKeyHandler.value());

            ChildKeyHandler<?> childKeyHandler = useChildKeyHandler.value().newInstance();
            Object value = childKeyHandler.handleChildKeys(subset(keyName + ".", config), configPath + "." + keyName);

            LOGGER.debug("Child key handler returned value {}", value);

            valueInjected |= coerceValueIntoField(rootObject, declaredField, value);
          } else if (isSimpleType(declaredField.getType())) {
            // Simple type, coerce value into the proper type
            LOGGER.debug("Coercing simple value type into field");
            valueInjected |= coerceValueIntoField(rootObject, declaredField, config.getOrElse(keyName, null));
          } else {
            // Complex type, recurse
            String suffix = keyName + ".";
            String newPath = configPath + suffix;
            LOGGER.debug("Recursively deserializing complex type");
            valueInjected |= coerceValueIntoField(rootObject, declaredField,
                deserialize(declaredField.getType(), subset(suffix, config), newPath));
          }
        } else if (isKeylessConfigField) {
          if (useChildKeyHandler != null) {
            // Use a child key handler to handle this value
            ChildKeyHandler<?> childKeyHandler = useChildKeyHandler.value().newInstance();
            Object value = childKeyHandler.handleChildKeys(config, configPath);
            valueInjected |= coerceValueIntoField(rootObject, declaredField, value);
          } else {
            // Complex type, recurse without changing the current path
            valueInjected |= coerceValueIntoField(rootObject, declaredField,
                deserialize(declaredField.getType(), config, configPath));
          }
        } else {
          // Skip this field, it is not annotated with @ConfigKey or @ConfigWithoutKey
        }
      } catch (Exception e) {
        LOGGER.warn("Caught exception while serializing field", e);
      }
    }

    // Return null if we did not write any fields in the destination object
    if (valueInjected) {
      // Post-inject hook?
      if (ConfigNodeLifecycleAware.class.isAssignableFrom(clazz)) {
        ((ConfigNodeLifecycleAware) rootObject).postInject();
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Returning deserialized value of {}", rootObject);
      }

      return rootObject;
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("No fields were written to the object of type {}, returning null", clazz.getName());
      }

      return null;
    }
  }

  private static <T> boolean coerceValueIntoField(T parentObject, Field field, Object value)
      throws ReflectiveOperationException {
    try {
      if (value == null) {
        return false;
      }

      Object destinationValue;
      Class<?> objectType = value.getClass();
      Class<?> fieldType = field.getType();

      if (objectType.equals(fieldType) || fieldType.isAssignableFrom(objectType)) {
        // Same type or assignable type, no conversion required
        destinationValue = value;
      } else if (fieldType.equals(String.class)) {
        // If destination field is a String, just do toString
        destinationValue = value.toString();
      } else if (fieldType.isArray()) {
        // Just assign arrays directly
        destinationValue = value;
      } else if (typeConverters.containsKey(Tuple.of(objectType, fieldType))) {
        // Use the appropriate converter
        Function<Object, Object> converter = typeConverters.get(Tuple.of(objectType, fieldType));
        destinationValue = converter.apply(value);
      } else {
        // TODO Use converters here
        if (value instanceof Number) {
          if (fieldType.isAssignableFrom(objectType)) { // object instanceof field
            destinationValue = value;
          } else if (Number.class.isAssignableFrom(fieldType) || fieldType.isPrimitive()) { // field instanceof Number
            Number numberValue = (Number) value;
            if (Integer.class.isAssignableFrom(fieldType) || int.class
                .isAssignableFrom(fieldType)) { // field instanceof int/Integer
              destinationValue = numberValue.intValue();
            } else {
              throw new RuntimeException("Unsupported conversion from " + objectType + " -> " + fieldType);
            }
          } else {
            throw new RuntimeException("Unsupported conversion from " + objectType + " -> " + fieldType);
          }
        } else if (value instanceof String) {
          String stringValue = (String) value;
          try {
            if (fieldType.isAssignableFrom(Integer.class) || fieldType.isAssignableFrom(int.class)) {
              destinationValue = Integer.parseInt(stringValue);
            } else if (fieldType.isAssignableFrom(Long.class) || fieldType.isAssignableFrom(long.class)) {
              destinationValue = Long.parseLong(stringValue);
            } else if (fieldType.isAssignableFrom(Boolean.class) || fieldType.isAssignableFrom(boolean.class)) {
              destinationValue = Boolean.parseBoolean(stringValue);
            } else if (Enum.class.isAssignableFrom(fieldType)) {
              destinationValue = Enum.valueOf(fieldType.asSubclass(Enum.class), stringValue.toUpperCase());
            } else if (fieldType.isAssignableFrom(Float.class) || fieldType.isAssignableFrom(float.class)) {
              destinationValue = Float.parseFloat(stringValue);
            } else if (fieldType.isAssignableFrom(Double.class) || fieldType.isAssignableFrom(double.class)) {
              destinationValue = Double.parseDouble(stringValue);
            } else if (fieldType.isAssignableFrom(Short.class) || fieldType.isAssignableFrom(short.class)) {
              destinationValue = Short.parseShort(stringValue);
            } else if (fieldType.isAssignableFrom(Byte.class) || fieldType.isAssignableFrom(byte.class)) {
              destinationValue = Byte.parseByte(stringValue);
            } else if (fieldType.isAssignableFrom(Character.class) || fieldType.isAssignableFrom(char.class)) {
              destinationValue = stringValue.charAt(0);
            }
            else {
              throw new RuntimeException("Unsupported conversion from " + objectType + " -> " + fieldType);
            }
          } catch (Exception e) {
            destinationValue = null;
          }
        } else if (value instanceof Boolean) {
          // TODO Fix this
          destinationValue = value;
        } else {
          throw new RuntimeException("Unsupported conversion from " + objectType + " -> " + fieldType);
        }
      }

      if (destinationValue != null) {
        field.setAccessible(true);
        field.set(parentObject, destinationValue);
        return true;
      }
    } catch (Exception e) {
      throw new ReflectiveOperationException(
          "Caught exception while processing field " + field.getName() + " of class " + field.getDeclaringClass(), e);
    }

    return false;
  }

  public static <T> T deserializeFromString(Class<T> clazz, String string) {
    Config config =
        ConfigFactory.parseString(string, ConfigParseOptions.defaults().prependIncluder(new ConfigIncluder() {
          private ConfigIncluder parent = null;

          public ConfigObject include(ConfigIncludeContext context, String what) {
            return ConfigFactory.parseFileAnySyntax(new File(what)).root();
          }

          public ConfigIncluder withFallback(ConfigIncluder fallback) {
            parent = fallback;
            return this;
          }
        }));

    config = config.resolve();

    try {
      return deserialize(clazz, io.vavr.collection.HashSet.ofAll(config.entrySet())
          .toMap(entry -> Tuple.of(entry.getKey(), entry.getValue().unwrapped())), "");
    } catch (Exception e) {
      Utils.rethrowException(e);
      return null;
    }
  }

  private static List<Field> getClassFields(Class<?> clazz) {
    List<Field> fields = List.of(clazz.getDeclaredFields());

    // Recursively add all parent fields
    while (clazz.getSuperclass() != null) {
      clazz = clazz.getSuperclass();
      fields = fields.appendAll(Arrays.asList(clazz.getDeclaredFields()));
    }

    return fields;
  }

  private static Map<String, ?> subset(String prefix, Map<String, ?> config) {
    final int prefixLength = prefix.length();
    return config.filter((key, value) -> key.startsWith(prefix))
        .map((key, value) -> Tuple.of(key.substring(prefixLength), value));
  }

  public static boolean isSimpleType(Class<?> fieldType) {
    return simpleTypes.contains(fieldType) || fieldType.equals(boolean.class) || fieldType.equals(String.class)
        || fieldType.equals(TimeUnit.class) || fieldType.isArray() || Enum.class.isAssignableFrom(fieldType);
  }
}

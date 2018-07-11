/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.vavr.Tuple;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import java.lang.reflect.Field;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Serializer, which takes an object and returns its representation as a map of key-value pairs or a string representing
 * a map of key-value pairs.
 */
public class Serializer {
  private static final Logger LOGGER = LoggerFactory.getLogger(Serializer.class);

  public static <T> Map<String, ?> serialize(T object) {
    try {
      if (object == null) {
        return null;
      }

      return serialize(object, object.getClass(), "");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> String serializeToString(T object) {
    Config config = ConfigFactory.parseMap(serialize(object).toJavaMap());
    return config.root().render(ConfigRenderOptions.defaults().setJson(false).setOriginComments(false));
  }

  private static <T> Map<String, ?> serialize(T object, Class<? extends T> clazz, String pathContext) throws Exception {
    if (object == null) {
      return HashMap.empty();
    }

    ConfigKey rootConfigKey = clazz.getAnnotation(ConfigKey.class);
    if (rootConfigKey != null) {
      if (pathContext.isEmpty()) {
        pathContext = rootConfigKey.value();
      } else {
        pathContext = pathContext + "." + rootConfigKey.value();
      }
    }

    Map<String, Object> values = HashMap.empty();

    Map<String, Map<String, Object>> dslValues = HashMap.empty();
    Map<String, Class<? extends SingleKeyDsl>> dslClasses = HashMap.empty();

    for (Field field : getClassFields(clazz)) {
      ConfigKey configKey = field.getAnnotation(ConfigKey.class);
      NestedConfig nestedConfig = field.getAnnotation(NestedConfig.class);
      UseDsl useDsl = field.getAnnotation(UseDsl.class);
      UseChildKeyHandler useChildKeyHandler = field.getAnnotation(UseChildKeyHandler.class);

      if (configKey == null && nestedConfig == null) {
        continue;
      }

      field.setAccessible(true);
      if (configKey != null) {
        final String keyName;
        if (pathContext.isEmpty()) {
          keyName = configKey.value();
        } else {
          keyName = pathContext + "." + configKey.value();
        }

        if (useDsl != null) {
          Map<String, Object> dslValue = dslValues.getOrElse(keyName, HashMap.empty());
          dslValue = dslValue.put(useDsl.value(), field.get(object));
          dslValues = dslValues.put(keyName, dslValue);
          dslClasses = dslClasses.put(keyName, useDsl.dsl());
        } else if (useChildKeyHandler != null) {
          ChildKeyHandler childKeyHandler = useChildKeyHandler.value().newInstance();
          Map serializedChildMap = childKeyHandler.unhandleChildKeys(field.get(object), keyName);
          if (serializedChildMap != null) {
            serializedChildMap = serializedChildMap.map((key, value) -> Tuple.of(keyName + "." + key, value));
            values = values.merge(serializedChildMap);
          }
        } else if (Deserializer.isSimpleType(field.getType())) {
          values = storeSimpleFieldIntoMap(field.get(object), field.getType(), keyName, values);
        } else {
          values = values.merge(serialize(field.get(object), field.getType(), keyName));
        }
      } else if (nestedConfig != null) {
        values = values.merge(serialize(field.get(object), field.getType(), pathContext));
      }
    }

    final Map<String, Class<? extends SingleKeyDsl>> finalDslClasses = dslClasses;
    final Map<String, String> dslUnparsedValues = dslValues.flatMap((configKey, dslValueData) -> {
      try {
        Class<? extends SingleKeyDsl> dslClass = finalDslClasses.getOrElse(configKey, null);
        SingleKeyDsl dslInstance = dslClass.newInstance();
        Class<?> dslValueType = dslClass.getMethod("parse", String.class).getReturnType();
        Object dslValueObject = Deserializer.deserialize(dslValueType, dslValueData, "");
        if (dslValueObject != null) {
          return List.of(Tuple.of(configKey, dslInstance.unparse(dslValueObject)));
        } else {
          return List.empty();
        }
      } catch (Exception e) {
        LOGGER.warn("Caught exception while serializing field for configKey {}", e, configKey);
        return List.empty();
      }
    });

    values = values.merge(dslUnparsedValues);

    UseChildKeyTransformers useChildKeyTransformers = clazz.getAnnotation(UseChildKeyTransformers.class);
    if (useChildKeyTransformers != null) {
      // Reverse the order of the child key transformers
      List<Class<? extends ChildKeyTransformer>> reversedChildKeyTransformers =
          List.ofAll(Arrays.asList(useChildKeyTransformers.value()))
          .reverse();

      for (Class<? extends ChildKeyTransformer> childKeyTransformerClass : reversedChildKeyTransformers) {
        LOGGER.debug("Using child key transformer {} on the root config {}", childKeyTransformerClass, values);

        ChildKeyTransformer childKeyTransformer = childKeyTransformerClass.newInstance();
        values = (Map<String, Object>) childKeyTransformer.unapply(values, pathContext);

        LOGGER.debug("Config after child key transformation {}", values);
      }
    }

    return values;
  }

  private static List<Field> getClassFields(Class<?> clazz) {
    List<Field> fields = List.of(clazz.getDeclaredFields());

    // Recursively add all parent fields
    while(clazz.getSuperclass() != null) {
      clazz = clazz.getSuperclass();
      fields = fields.appendAll(Arrays.asList(clazz.getDeclaredFields()));
    }

    return fields;
  }

  private static Map<String, Object> storeSimpleFieldIntoMap(Object object, Class<?> type, String keyName, Map<String, Object> values) {
    // No object to write
    if (object == null) {
      return values;
    }

    if (Enum.class.isAssignableFrom(type)) {
      object = object.toString();
    }

    return values.put(keyName, object);
  }
}

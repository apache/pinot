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
package org.apache.pinot.segment.local.segment.store;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.V1Constants.Indexes;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TextIndexUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(TextIndexUtils.class);
  private TextIndexUtils() {
  }

  static void cleanupTextIndex(File segDir, String column) {
    // Remove the lucene index file and potentially the docId mapping file.
    File luceneIndexFile = new File(segDir, column + Indexes.LUCENE_TEXT_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(luceneIndexFile);
    File luceneMappingFile = new File(segDir, column + Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    FileUtils.deleteQuietly(luceneMappingFile);
    File luceneV9IndexFile = new File(segDir, column + Indexes.LUCENE_V9_TEXT_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(luceneV9IndexFile);
    File luceneV99IndexFile = new File(segDir, column + Indexes.LUCENE_V99_TEXT_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(luceneV99IndexFile);
    File luceneV912IndexFile = new File(segDir, column + Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(luceneV912IndexFile);
    File luceneV9MappingFile = new File(segDir, column + Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    FileUtils.deleteQuietly(luceneV9MappingFile);

    // Remove the native index file
    File nativeIndexFile = new File(segDir, column + Indexes.NATIVE_TEXT_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(nativeIndexFile);
  }

  static boolean hasTextIndex(File segDir, String column) {
    //@formatter:off
    return new File(segDir, column + Indexes.LUCENE_TEXT_INDEX_FILE_EXTENSION).exists()
        || new File(segDir, column + Indexes.LUCENE_V9_TEXT_INDEX_FILE_EXTENSION).exists()
        || new File(segDir, column + Indexes.NATIVE_TEXT_INDEX_FILE_EXTENSION).exists()
        || new File(segDir, column + Indexes.LUCENE_V99_TEXT_INDEX_FILE_EXTENSION).exists()
        || new File(segDir, column + Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION).exists();
    //@formatter:on
  }

  public static boolean isFstTypeNative(@Nullable Map<String, String> textIndexProperties) {
    if (textIndexProperties == null) {
      return false;
    }
    for (Map.Entry<String, String> entry : textIndexProperties.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(FieldConfig.TEXT_FST_TYPE)) {
        return entry.getValue().equalsIgnoreCase(FieldConfig.TEXT_NATIVE_FST_LITERAL);
      }
    }
    return false;
  }

  public static FSTType getFSTTypeOfIndex(File indexDir, String column) {
    return SegmentDirectoryPaths.findTextIndexIndexFile(indexDir, column) != null ? FSTType.LUCENE : FSTType.NATIVE;
  }

  public static List<String> extractStopWordsInclude(String colName,
      Map<String, Map<String, String>> columnProperties) {
    return extractStopWordsInclude(columnProperties.getOrDefault(colName, null));
  }

  public static List<String> extractStopWordsExclude(String colName,
      Map<String, Map<String, String>> columnProperties) {
    return extractStopWordsExclude(columnProperties.getOrDefault(colName, null));
  }

  public static List<String> extractStopWordsInclude(Map<String, String> columnProperty) {
    return parseEntryAsString(columnProperty, FieldConfig.TEXT_INDEX_STOP_WORD_INCLUDE_KEY);
  }

  public static List<String> extractStopWordsExclude(Map<String, String> columnProperty) {
    return parseEntryAsString(columnProperty, FieldConfig.TEXT_INDEX_STOP_WORD_EXCLUDE_KEY);
  }

  private static List<String> parseEntryAsString(@Nullable Map<String, String> columnProperties, String stopWordKey) {
    if (columnProperties == null) {
      return Collections.emptyList();
    }
    String includeWords = columnProperties.getOrDefault(stopWordKey, "");
    return Arrays.stream(includeWords.split(FieldConfig.TEXT_INDEX_STOP_WORD_SEPERATOR)).map(String::trim)
        .collect(Collectors.toList());
  }

  /**
   * Retrieves the Lucene Analyzer class instance via reflection from the fully qualified class name of the text config.
   * If the class name is not specified in the config, the default StandardAnalyzer is instantiated.
   *
   * @param config Pinot TextIndexConfig to fetch the configuration from
   * @return Lucene Analyzer class instance
   * @throws ReflectiveOperationException if instantiation via reflection fails
   */
  public static Analyzer getAnalyzer(TextIndexConfig config) throws ReflectiveOperationException {
    String luceneAnalyzerClassName = config.getLuceneAnalyzerClass();
    List<String> luceneAnalyzerClassArgs = config.getLuceneAnalyzerClassArgs();
    List<String> luceneAnalyzerClassArgTypes = config.getLuceneAnalyzerClassArgTypes();

    if (null == luceneAnalyzerClassName || luceneAnalyzerClassName.isEmpty()
            || (luceneAnalyzerClassName.equals(StandardAnalyzer.class.getName())
                    && luceneAnalyzerClassArgs.isEmpty() && luceneAnalyzerClassArgTypes.isEmpty())) {
      // When there is no analyzer defined, or when StandardAnalyzer (default) is used without arguments,
      // use existing logic to obtain an instance of StandardAnalyzer with customized stop words
      return TextIndexUtils.getStandardAnalyzerWithCustomizedStopWords(
              config.getStopWordsInclude(), config.getStopWordsExclude());
    }

    // Custom analyzer + custom configs via reflection
    if (luceneAnalyzerClassArgs.size() != luceneAnalyzerClassArgTypes.size()) {
      throw new ReflectiveOperationException("Mismatch of the number of analyzer arguments and arguments types.");
    }

    // Generate args type list
    List<Class<?>> argClasses = new ArrayList<>();
    for (String argType : luceneAnalyzerClassArgTypes) {
      argClasses.add(parseSupportedTypes(argType));
    }

    // Best effort coercion to the analyzer argument type
    // Note only a subset of class types is supported, unsupported ones can be added in the future
    List<Object> argValues = new ArrayList<>();
    for (int i = 0; i < luceneAnalyzerClassArgs.size(); i++) {
      argValues.add(parseSupportedTypeValues(luceneAnalyzerClassArgs.get(i), argClasses.get(i)));
    }

    // Initialize the custom analyzer class with custom analyzer args
    Class<?> luceneAnalyzerClass = Class.forName(luceneAnalyzerClassName);
    if (!Analyzer.class.isAssignableFrom(luceneAnalyzerClass)) {
      String exceptionMessage = "Custom analyzer must be a child of " + Analyzer.class.getCanonicalName();
      LOGGER.error(exceptionMessage);
      throw new ReflectiveOperationException(exceptionMessage);
    }

    // Return a new instance of custom lucene analyzer class
    return (Analyzer) luceneAnalyzerClass.getConstructor(argClasses.toArray(new Class<?>[0]))
            .newInstance(argValues.toArray(new Object[0]));
  }

  /**
   * Parse the Java value type specified in the type string
   * @param valueTypeString FQCN of the value type class or the name of the primitive value type
   * @return Class object of the value type
   * @throws ClassNotFoundException when the value type is not supported
   */
  public static Class<?> parseSupportedTypes(String valueTypeString) throws ClassNotFoundException {
    try {
      // Support both primitive types + class
      switch (valueTypeString) {
        case "java.lang.Byte.TYPE":
          return Byte.TYPE;
        case "java.lang.Short.TYPE":
          return Short.TYPE;
        case "java.lang.Integer.TYPE":
          return Integer.TYPE;
        case "java.lang.Long.TYPE":
          return Long.TYPE;
        case "java.lang.Float.TYPE":
          return Float.TYPE;
        case "java.lang.Double.TYPE":
          return Double.TYPE;
        case "java.lang.Boolean.TYPE":
          return Boolean.TYPE;
        case "java.lang.Character.TYPE":
          return Character.TYPE;
        default:
          return Class.forName(valueTypeString);
      }
    } catch (ClassNotFoundException ex) {
      LOGGER.error("Analyzer argument class type not found: " + valueTypeString);
      throw ex;
    }
  }

  /**
   * Attempt to coerce string into supported value type
   * @param stringValue string representation of the value
   * @param clazz of the value
   * @return class object of the value, auto-boxed if it is a primitive type
   * @throws ReflectiveOperationException if value cannot be coerced without ambiguity or encountered unsupported type
   */
  public static Object parseSupportedTypeValues(String stringValue, Class<?> clazz)
          throws ReflectiveOperationException {
    try {
      if (clazz.equals(String.class)) {
        return stringValue;
      } else if (clazz.equals(Byte.class) || clazz.equals(Byte.TYPE)) {
        return Byte.parseByte(stringValue);
      } else if (clazz.equals(Short.class) || clazz.equals(Short.TYPE)) {
        return Short.parseShort(stringValue);
      } else if (clazz.equals(Integer.class) || clazz.equals(Integer.TYPE)) {
        return Integer.parseInt(stringValue);
      } else if (clazz.equals(Long.class) || clazz.equals(Long.TYPE)) {
        return Long.parseLong(stringValue);
      } else if (clazz.equals(Float.class) || clazz.equals(Float.TYPE)) {
        return Float.parseFloat(stringValue);
      } else if (clazz.equals(Double.class) || clazz.equals(Double.TYPE)) {
        return Double.parseDouble(stringValue);
      } else if (clazz.equals(Boolean.class) || clazz.equals(Boolean.TYPE)) {
        // Note we cannot use Boolean.parseBoolean here because it treats "abc" as false which
        // introduces unexpected parsing results. We should validate the input by accepting only
        // true|false in a case-insensitive manner, for all other values, return an exception.
        String lowerCaseStringValue = stringValue.toLowerCase();
        if (lowerCaseStringValue.equals("true")) {
          return true;
        } else if (lowerCaseStringValue.equals("false")) {
          return false;
        }
        throw new ReflectiveOperationException();
      } else if (clazz.equals(Character.class) || clazz.equals(Character.TYPE)) {
        if (stringValue.length() == 1) {
          return stringValue.charAt(0);
        }
        throw new ReflectiveOperationException();
      } else {
        throw new UnsupportedOperationException();
      }
    } catch (NumberFormatException | ReflectiveOperationException ex) {
      String exceptionMessage = "Custom analyzer argument cannot be coerced from "
              + stringValue + " to " + clazz.getName() + " type";
      LOGGER.error(exceptionMessage);
      throw new ReflectiveOperationException(exceptionMessage);
    } catch (UnsupportedOperationException ex) {
      // In the future, consider adding more common serdes for common complex types used within Lucene
      String exceptionMessage = "Custom analyzer argument does not support " + clazz.getName() + " type";
      LOGGER.error(exceptionMessage);
      throw new ReflectiveOperationException(exceptionMessage);
    }
  }

  public static StandardAnalyzer getStandardAnalyzerWithCustomizedStopWords(@Nullable List<String> stopWordsInclude,
      @Nullable List<String> stopWordsExclude) {
    HashSet<String> stopWordSet = LuceneTextIndexCreator.getDefaultEnglishStopWordsSet();
    if (stopWordsInclude != null) {
      stopWordSet.addAll(stopWordsInclude);
    }
    if (stopWordsExclude != null) {
      stopWordsExclude.forEach(stopWordSet::remove);
    }
    return new StandardAnalyzer(new CharArraySet(stopWordSet, true));
  }

  public static Constructor<QueryParserBase> getQueryParserWithStringAndAnalyzerTypeConstructor(
          String queryParserClassName) throws ReflectiveOperationException {
    // Fail-fast if the query parser is specified class is not QueryParseBase class
    final Class<?> queryParserClass = Class.forName(queryParserClassName);
    if (!QueryParserBase.class.isAssignableFrom(queryParserClass)) {
      throw new ReflectiveOperationException(
          "The specified lucene query parser class " + queryParserClassName + " is not assignable from "
              + QueryParserBase.class.getName());
    }
    // Fail-fast if the query parser does not have the required constructor used by this class
    try {
      queryParserClass.getConstructor(String.class, Analyzer.class);
    } catch (NoSuchMethodException ex) {
      throw new NoSuchMethodException("The specified lucene query parser class " + queryParserClassName
          + " is not assignable from does not have the required constructor method with parameter type "
          + "[String.class, Analyzer.class]");
    }

    return (Constructor<QueryParserBase>) queryParserClass.getConstructor(String.class, Analyzer.class);
  }

  /**
   * Writes the config to the properties file. Configs saved include luceneAnalyzerClass, luceneAnalyzerClassArgs,
   * luceneAnalyzerClassArgTypes, and luceneQueryParserClass.
   *
   * @param indexDir directory where the properties file is saved
   * @param config config to write to the properties file
   */
  public static void writeConfigToPropertiesFile(File indexDir, TextIndexConfig config) {
    PropertiesConfiguration properties = new PropertiesConfiguration();
    List<String> escapedLuceneAnalyzerClassArgs = config.getLuceneAnalyzerClassArgs().stream()
        .map(CommonsConfigurationUtils::replaceSpecialCharacterInPropertyValue).collect(Collectors.toList());
    List<String> escapedLuceneAnalyzerClassArgTypes = config.getLuceneAnalyzerClassArgTypes().stream()
        .map(CommonsConfigurationUtils::replaceSpecialCharacterInPropertyValue).collect(Collectors.toList());

    properties.setProperty(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS, config.getLuceneAnalyzerClass());
    properties.setProperty(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARGS, escapedLuceneAnalyzerClassArgs);
    properties.setProperty(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARG_TYPES, escapedLuceneAnalyzerClassArgTypes);
    properties.setProperty(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, config.getLuceneQueryParserClass());

    File propertiesFile = new File(indexDir, V1Constants.Indexes.LUCENE_TEXT_INDEX_PROPERTIES_FILE);
    CommonsConfigurationUtils.saveToFile(properties, propertiesFile);
  }

  /**
   * Returns an updated TextIndexConfig, overriding the values in the config with the values in the properties file.
   * The configs overwritten include luceneAnalyzerClass, luceneAnalyzerClassArgs, luceneAnalyzerClassArgTypes,
   * and luceneQueryParserClass.
   *
   * @param file properties file to read from
   * @param config config to update
   * @return updated TextIndexConfig
   */
  public static TextIndexConfig getUpdatedConfigFromPropertiesFile(File file, TextIndexConfig config)
      throws ConfigurationException {
    PropertiesConfiguration properties = CommonsConfigurationUtils.fromFile(file);
    List<String> luceneAnalyzerClassArgs =
        properties.getList(String.class, FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARGS);
    List<String> luceneAnalyzerClassArgTypes =
        properties.getList(String.class, FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARG_TYPES);
    List<String> recoveredLuceneAnalyzerClassArgs = luceneAnalyzerClassArgs == null ? new ArrayList<>()
        : luceneAnalyzerClassArgs.stream().map(CommonsConfigurationUtils::recoverSpecialCharacterInPropertyValue)
            .collect(Collectors.toList());
    List<String> recoveredLuceneAnalyzerClassArgTypes = luceneAnalyzerClassArgTypes == null ? new ArrayList<>()
        : luceneAnalyzerClassArgTypes.stream().map(CommonsConfigurationUtils::recoverSpecialCharacterInPropertyValue)
            .collect(Collectors.toList());

    return new TextIndexConfigBuilder(config).withLuceneAnalyzerClass(
            properties.getString(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS))
        .withLuceneAnalyzerClassArgs(recoveredLuceneAnalyzerClassArgs)
        .withLuceneAnalyzerClassArgTypes(recoveredLuceneAnalyzerClassArgTypes)
        .withLuceneQueryParserClass(properties.getString(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS)).build();
  }
}

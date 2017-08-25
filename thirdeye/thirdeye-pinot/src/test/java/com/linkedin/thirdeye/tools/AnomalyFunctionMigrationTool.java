package com.linkedin.thirdeye.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.detector.function.AnomalyFunction;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This tool is designed to migrate a batch of anomaly function to a new function type
 * In Q2 2017, we developed a modularized anomaly detection framework. In Q2 and Q3, we have successfully refactored all
 * the detection models to the new framework. This tool is designed to migrate the existing anomaly detection function
 * to the new framework.
 *
 * This tool contains four parts:
 *  - function type change:
 *      change the function type to the new function type. The anomaly function factory is to verify the validity of
 *      the new function
 *  - properties migration:
 *      move the old properties to the new properties. This takes advantage of a propertyKeyMap: the maps tells the tool
 *      what is the new name of the old property key. If the name is unchanged, the tool check the key from the function
 *      definition.
 *  - additional properties:
 *      if the new model requires a new set of properties. This will override the old properties, if exist.
 *  - modify function configs:
 *      if the new model requires a new set of function configs.
 */
public class AnomalyFunctionMigrationTool extends BaseThirdEyeTool {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyFunctionMigrationTool.class);
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static String PATH_TO_ANOMALY_FUNCTION_PROPERTIES = "detector-config/anomaly-functions/functions.properties";
  public static String PATH_TO_PERSISTENCE = "persistence.yml";

  private Map<String, String> propertyKeyMap;
  private Map<String, String> overrideProperties;
  private Map<String, String> overrideConfigs;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private AnomalyFunctionManager anomalyFunctionDAO;

  /**
   * Construct migration tool
   * @param configPath
   *  the path to the folder of config files
   */
  public AnomalyFunctionMigrationTool(String configPath) {
    File configFolder = new File(configPath);
    setAnomalyFunctionFactory((new File(configFolder, PATH_TO_ANOMALY_FUNCTION_PROPERTIES)).getAbsolutePath());
    init((new File(configFolder, PATH_TO_PERSISTENCE)));
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    this.propertyKeyMap = Collections.emptyMap();
    this.overrideProperties = Collections.emptyMap();
    this.overrideConfigs = Collections.emptyMap();
  }

  public void setAnomalyFunctionFactory(String configPaht) {
    anomalyFunctionFactory = new AnomalyFunctionFactory(configPaht);
  }

  public void setPropertyKeyMap(String json) throws IOException {
    setPropertyKeyMap(OBJECT_MAPPER.readValue(json, HashMap.class));
  }

  public void setPropertyKeyMap(Map<String, String> map) {
    this.propertyKeyMap = map;
  }

  public Map<String, String> getPropertyKeyMap() {
    return this.propertyKeyMap;
  }

  public Map<String, String> getOverrideProperties() {
    return overrideProperties;
  }

  public void setOverrideProperties(Map<String, String> overrideProperties) {
    this.overrideProperties = overrideProperties;
  }

  public void setOverrideProperties(String json) throws IOException {
    setPropertyKeyMap(OBJECT_MAPPER.readValue(json, HashMap.class));
  }

  public Map<String, String> getOverrideConfigs() {
    return overrideConfigs;
  }

  public void setOverrideConfigs(Map<String, String> overrideConfigs) {
    this.overrideConfigs = overrideConfigs;
  }

  public void setOverrideConfigs(String json) throws IOException {
    setPropertyKeyMap(OBJECT_MAPPER.readValue(json, HashMap.class));
  }

  /**
   * Migrate all the anomaly function with srcFunctionType to the destFunctionType
   * @param srcFunctionType
   * @param destFunctionType
   */
  public void migrateTo(String srcFunctionType, String destFunctionType) {
    List<AnomalyFunctionDTO> anomalyFunctions = new ArrayList<>();
    for (AnomalyFunctionDTO anomalyFunction : anomalyFunctionDAO.findAll()) {
      if (anomalyFunction.getType().equalsIgnoreCase(srcFunctionType)) {
        anomalyFunctions.add(anomalyFunction);
      }
    }
    migrate(anomalyFunctions, destFunctionType);
  }

  /**
   * Migrate a list of anomaly function with given id to the destFunctionType
   * @param functionIds
   * @param destFunctionType
   */
  public void migrateTo(List<Long> functionIds, String destFunctionType) {
    List<AnomalyFunctionDTO> anomalyFunctions = new ArrayList<>();
    for (long functionId : functionIds) {
      AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
      if (anomalyFunction != null) {
        anomalyFunctions.add(anomalyFunction);
      }
    }
    migrate(anomalyFunctions, destFunctionType);
  }

  /**
   * Migrate a list of anomaly function instances to the destFunctionType
   * @param anomalyFunctions
   * @param destFunctionType
   */
  private void migrate(List<AnomalyFunctionDTO> anomalyFunctions, String destFunctionType) {
    for (AnomalyFunctionDTO anomalyFunction : anomalyFunctions) {
      migrate(anomalyFunction, destFunctionType);
    }
  }

  /**
   * Migrate an anomaly function instance to the destFunctionType
   * @param anomalyFunction
   * @param destFunctionType
   */
  private void migrate(AnomalyFunctionDTO anomalyFunction, String destFunctionType) {
    LOG.info("Migrating anomaly function {} from type: {} to type: {}", anomalyFunction.getId(),
        anomalyFunction.getType(), destFunctionType);
    String originalType = anomalyFunction.getType();
    anomalyFunction.setType(destFunctionType);
    if (isValid(anomalyFunction)) {
      // Update the function properties: 1) move old properties, and 2) assign new properties
      try {
        updateProperties(anomalyFunction);
      } catch (Exception e) {
        LOG.error("Anomaly function ({}) is invalid", anomalyFunction, e);
        return;
      }
      // Update function configs
      try {
        updateFunctionConfig(anomalyFunction);
      } catch (NoSuchFieldException e) {
        LOG.error("There is a invalid field in the config", e);
        return;
      } catch (IllegalAccessException e) {
        LOG.error("Unable to access to a field", e);
        return;
      }
      /*
       Save the change
       Unless all the required actions are made successfully, the change is updated.
        */
      anomalyFunctionDAO.update(anomalyFunction);
    } else {
      LOG.warn("Unable to change anomaly function {} to new function type. Changes have been reverted.", anomalyFunction.getId());
      anomalyFunction.setType(originalType);
    }
  }

  /**
   * Check if given anomaly function type is valid
   * @param anomalyFunction
   * @return
   */
  private boolean isValid(AnomalyFunctionDTO anomalyFunction) {
    try {
      anomalyFunctionFactory.fromSpec(anomalyFunction);
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  /**
   * Update function properties
   *  - Move old properties to the new ones with new property name
   *  - Move old properties which are still required in the new function
   *  - Assign new properties to the new function
   * @param anomalyFunction
   * @throws Exception
   */
  private void updateProperties(AnomalyFunctionDTO anomalyFunction) throws Exception {
    Properties oldProperties = anomalyFunction.toProperties();
    AnomalyFunction anomalyFunc = anomalyFunctionFactory.fromSpec(anomalyFunction);
    List<String> requiredPropertyKeys = Arrays.asList(anomalyFunc.getPropertyKeys());

    // Move old properties to new properties
    Map<String, String> newProperties = new HashMap<>();
    for (String propertyName : oldProperties.stringPropertyNames()) {
      if (propertyKeyMap.containsKey(propertyName)) {
        newProperties.put(propertyKeyMap.get(propertyName), oldProperties.getProperty(propertyName));
      }
      // If the old property key is also listed in the new function, move the old property key with its original name
      if (requiredPropertyKeys.contains(propertyName)) {
        newProperties.put(propertyName, oldProperties.getProperty(propertyName));
      }
    }

    // Apply customized user-definition to new Properties
    newProperties.putAll(overrideProperties);

    // Clean up the old properties
    anomalyFunction.setProperties("");
    anomalyFunction.updateProperties(newProperties);
  }

  /**
   * Apply user defined function configs
   * User assign new function configs in the format of String-String. The function parses the String value according to
   * the field type. Then the new field value is assign to the field.
   *
   * Note that, this function is good to assign new config with generic type. If user wants to assign value
   * @param anomalyFunction
   * @throws Exception
   */
  private void updateFunctionConfig(AnomalyFunctionDTO anomalyFunction) throws NoSuchFieldException, IllegalAccessException{
    // Get the class reflection
    Class<?> clazz = anomalyFunction.getClass();
    for (Map.Entry<String, String> entry : this.overrideConfigs.entrySet()) {
      Field functionField = null;
      // get the field from the class, either in AnomalyFunctionDTO or in AnomalyFunctionBean
      try {
        functionField = clazz.getDeclaredField(entry.getKey());
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
        functionField = clazz.getDeclaredField(entry.getKey());
      }
      // The the type of the field
      Class<?> attributeType = functionField.getType();
      // Save the original accessibility and allow access for now
      boolean accessibility = functionField.isAccessible();
      functionField.setAccessible(true);
      // Assign the filed value to the field. If the type is not supported, user need to define their own way
      PropertyEditor propertyEditor = PropertyEditorManager.findEditor(attributeType);
      if (propertyEditor != null) {
        propertyEditor.setAsText(entry.getValue());
        functionField.set(anomalyFunction, propertyEditor.getValue());
      } else { // Cannot find a propertyEditor to cast String to the required field value
        if (attributeType.equals(TimeGranularity.class)) {
          functionField.set(anomalyFunction, TimeGranularity.fromString(entry.getValue()));
        } else if (attributeType.equals(TimeUnit.class)) {
          functionField.set(anomalyFunction, TimeUnit.valueOf(entry.getValue()));
        } else if (attributeType.equals(Map.class)) {
          try {
            functionField.set(anomalyFunction, OBJECT_MAPPER.readValue(entry.getValue(), HashMap.class));
          } catch (IOException e) {
            LOG.warn("Unable to cast value to the filed {} to Map. The field type {} is not support", entry.getKey(), attributeType);
          }
        } else {
          LOG.warn("Unable to assign value to the filed {}. The field type {} is not support", entry.getKey(), attributeType);
        }
      }
      // revert the field accessibility
      functionField.setAccessible(accessibility);
    }
  }

  public static void main(String[] args) throws Exception{
    if (args.length != 3) {
      LOG.warn("Invalid number of parameters.\nShould be in format of <config_path> <source_function_type>"
          + " <destination_function_type>");
    }

    String configPath = args[0];
    String srcFunctionType = args[1];
    String destFunctionType = args[2];

    AnomalyFunctionMigrationTool tool = new AnomalyFunctionMigrationTool(configPath);
  }
}

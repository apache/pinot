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

package com.linkedin.thirdeye.anomaly.classification.classifier;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnomalyClassifierFactory {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyClassifierFactory.class);
  private static final AnomalyClassifier DUMMY_ANOMALY_CLASSIFIER = new DummyAnomalyClassifier();
  public static final String ANOMALY_CLASSIFIER_TYPE_KEY = "type";

  private final Properties props = new Properties();

  /**
   * Instantiates a default factory which only has the implementation of internal classes in ThirdEye project.
   */
  public AnomalyClassifierFactory() {
  }

  /**
   * Instantiates a factory that has the configuration file, which is given by a file path, to external classes.
   */
  public AnomalyClassifierFactory(String configPath) {
    try {
      InputStream input = new FileInputStream(configPath);
      loadPropertiesFromInputStream(input);
    } catch (FileNotFoundException e) {
      LOG.warn(
          "Property file ({}) to external classifier is not found; Only internal implementations will be available in this factory.",
          configPath, e);
    }
  }

  /**
   * Instantiates a factory that has the configuration file, which is given as an input stream, to external classes.
   */
  public AnomalyClassifierFactory(InputStream input) {
    loadPropertiesFromInputStream(input);
  }

  /**
   * Loads the configuration file to external classes.
   *
   * @param input the input steam of the configuration file.
   */
  private void loadPropertiesFromInputStream(InputStream input) {
    try {
      props.load(input);
    } catch (IOException e) {
      LOG.error("Error loading the alert filters from config", e);
    } finally {
      IOUtils.closeQuietly(input);
    }
  }

  /**
   * Given a spec, which is a map of string to a string, of the anomaly classifier, returns a anomaly classifier
   * instance. The implementation of the instance could be located in an external package or ThirdEye project.
   *
   * If the type of provider exists in both the external package and ThirdEye project, then the implementation from
   * external package will be used.
   *
   * @param spec a map of string to a string.
   *
   * @return a anomaly classifier instance.
   */
  public AnomalyClassifier fromSpec(Map<String, String> spec) {
    if (spec == null) {
      spec = Collections.emptyMap();
    }
    // the default recipient provider is a dummy recipient provider
    AnomalyClassifier anomalyClassifier = DUMMY_ANOMALY_CLASSIFIER;
    if (spec.containsKey(ANOMALY_CLASSIFIER_TYPE_KEY)) {
      String classifierType = spec.get(ANOMALY_CLASSIFIER_TYPE_KEY);
      // We first check if the implementation (class) of this provider comes from external packages
      if(props.containsKey(classifierType.toUpperCase())) {
        String className = props.getProperty(classifierType.toUpperCase());
        try {
          anomalyClassifier = (AnomalyClassifier) Class.forName(className).newInstance();
        } catch (Exception e) {
          LOG.warn(e.getMessage());
        }
      } else { // otherwise, we try to look for the implementation from internal classes
        anomalyClassifier = fromStringType(spec.get(ANOMALY_CLASSIFIER_TYPE_KEY));
      }
    }
    anomalyClassifier.setParameters(spec);

    return anomalyClassifier;
  }

  /**
   * The available classifier type in ThirdEye project.
   */
  public enum AnomalyClassifierType {
    DUMMY
  }

  /**
   * The methods returns the instance of anomaly classifier whose implementation is located in ThirdEye project.
   *
   * @param type the type name of the provider.
   *
   * @return a instance of anomaly classifier.
   */
  private static AnomalyClassifier fromStringType(String type) {
    if (StringUtils.isBlank(type)) { // safety check and backward compatibility
      return DUMMY_ANOMALY_CLASSIFIER;
    }

    AnomalyClassifierType classifierType = AnomalyClassifierType.DUMMY;
    for (AnomalyClassifierType enumClassifierType : AnomalyClassifierType.values()) {
      if (enumClassifierType.name().compareToIgnoreCase(type) == 0) {
        classifierType = enumClassifierType;
        break;
      }
    }

    switch (classifierType) {
    case DUMMY: // speed optimization for most use cases
    default:
      return DUMMY_ANOMALY_CLASSIFIER;
    }
  }
}

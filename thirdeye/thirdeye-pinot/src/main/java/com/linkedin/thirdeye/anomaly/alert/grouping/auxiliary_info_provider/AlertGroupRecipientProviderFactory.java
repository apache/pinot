package com.linkedin.thirdeye.anomaly.alert.grouping.auxiliary_info_provider;

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

/**
 * This factory instantiates the implementation (class) of AlertGroupAuxiliaryInfoProvider from two sources: 1. The
 * classes that are specified from a configuration file. 2. The classes in ThirdEye open source project. Given
 * the type of recipient provider, this class tries the first type of classes using Java reflection; if it fails,
 * then it looks for the internal classes.
 *
 * The configuration file to external classes is formatted in the following form:
 *   SHORT_TYPE_NAME1=the.full.class.name1
 *   SHORT_TYPE_NAME2=the.full.class.name2
 *   ...
 */
public class AlertGroupRecipientProviderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(AlertGroupRecipientProviderFactory.class);
  private static final AlertGroupAuxiliaryInfoProvider DUMMY_ALERT_GROUP_RECIPIENT_PROVIDER =
      new DummyAlertGroupAuxiliaryInfoProvider();
  public static final String GROUP_RECIPIENT_PROVIDER_TYPE_KEY = "type";

  private final Properties props = new Properties();

  /**
   * The default constructor that instantiates a factory that does not have the configuration file to external classes.
   */
  public AlertGroupRecipientProviderFactory() { }

  /**
   * The constructor that instantiates a factory that has the configuration file to external classes. The configuration
   * file is given by its path.
   */
  public AlertGroupRecipientProviderFactory(String alertGroupRecipientProviderConfigPath) {
    try {
      InputStream input = new FileInputStream(alertGroupRecipientProviderConfigPath);
      loadPropertiesFromInputStream(input);
    } catch (FileNotFoundException e) {
      LOG.warn(
          "Property file ({}) to external recipient provider is not found; Only internal implementations will be available in this factory.",
          alertGroupRecipientProviderConfigPath, e);
    }
  }

  /**
   * The constructor that instantiates a factory that has the configuration file to external classes. The configuration
   * file is given as an input stream.
   */
  public AlertGroupRecipientProviderFactory(InputStream input) {
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
   * Given a spec, which is a map of string to a string, of the recipient provider, returns a recipient provider
   * instance. The implementation of the instance could be located in an external package or ThirdEye project.
   *
   * If the type of provider exists in both the external package and ThirdEye project, then the implementation from
   * external package will be used.
   *
   * @param spec a map of string to a string.
   *
   * @return a recipient provider instance.
   */
  public AlertGroupAuxiliaryInfoProvider fromSpec(Map<String, String> spec) {
    if (spec == null) {
      spec = Collections.emptyMap();
    }
    // the default recipient provider is a dummy recipient provider
    AlertGroupAuxiliaryInfoProvider recipientProvider = DUMMY_ALERT_GROUP_RECIPIENT_PROVIDER;
    if (spec.containsKey(GROUP_RECIPIENT_PROVIDER_TYPE_KEY)) {
      String recipientProviderType = spec.get(GROUP_RECIPIENT_PROVIDER_TYPE_KEY);
      // We first check if the implementation (class) of this provider comes from external packages
      if(props.containsKey(recipientProviderType.toUpperCase())) {
        String className = props.getProperty(recipientProviderType.toUpperCase());
        try {
          recipientProvider = (AlertGroupAuxiliaryInfoProvider) Class.forName(className).newInstance();
        } catch (Exception e) {
          LOG.warn(e.getMessage());
        }
      } else { // otherwise, we try to look for the implementation from internal classes
        recipientProvider = fromStringType(spec.get(GROUP_RECIPIENT_PROVIDER_TYPE_KEY));
      }
    }
    recipientProvider.setParameters(spec);

    return recipientProvider;
  }

  public enum GroupRecipientProviderType {
    DUMMY, DIMENSIONAL
  }

  /**
   * The methods returns the instance of recipient provider whose implementation is located in ThirdEye project.
   *
   * @param type the type name of the provider.
   *
   * @return a instance of recipient provider.
   */
  private static AlertGroupAuxiliaryInfoProvider fromStringType(String type) {
    if (StringUtils.isBlank(type)) { // safety check and backward compatibility
      return DUMMY_ALERT_GROUP_RECIPIENT_PROVIDER;
    }

    AlertGroupRecipientProviderFactory.GroupRecipientProviderType providerType = GroupRecipientProviderType.DUMMY;
    for (GroupRecipientProviderType enumProviderType : GroupRecipientProviderType.values()) {
      if (enumProviderType.name().compareToIgnoreCase(type) == 0) {
        providerType = enumProviderType;
        break;
      }
    }

    switch (providerType) {
    case DUMMY: // speed optimization for most use cases
      return DUMMY_ALERT_GROUP_RECIPIENT_PROVIDER;
    case DIMENSIONAL:
      return new DimensionalAlertGroupAuxiliaryRecipientProvider();
    default:
      return DUMMY_ALERT_GROUP_RECIPIENT_PROVIDER;
    }
  }
}

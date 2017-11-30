package com.linkedin.thirdeye.detector.email.filter;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class BaseAlertFilter implements AlertFilter {
  private final static Logger LOG = LoggerFactory.getLogger(BaseAlertFilter.class);


  /**
   * Parses the parameter setting for this filter.
   *
   * This method goes through the parameters defined by the method AlertFilter.getPropertyNames() of each AlertFilter
   * class and get the parameter value from the given parameter setting. If a parameter (property) is missing, then it
   * gets the default value, which is defined within the corresponding filter class with prefix "DEFAULT_". For example,
   * for a parameter whose field name is "Abc_Def", its default value has to have this name "DEFAULT_ABC_DEF".
   *
   * @param parameterSetting a mapping from field name to user specified value for that field
   */
  @Override
  public void setParameters(Map<String, String> parameterSetting) {
    Class c = this.getClass();
    for (String fieldName : getPropertyNames()) {
      Double value = null;
      String fieldVal = null;
      // Get user's value for the specified field
      if (parameterSetting.containsKey(fieldName)) {
        fieldVal = parameterSetting.get(fieldName);
        if (NumberUtils.isNumber(fieldVal)) {
          value = Double.parseDouble(parameterSetting.get(fieldName));
        }
      } else {
        // If user's value does not exist, try to get the default value from Class definition
        try {
          Field field = c.getDeclaredField(fieldName);
          boolean accessible = field.isAccessible();
          field.setAccessible(true);
          if (field.getType().equals(Double.class) || field.getType().equals(double.class)) {
            value = (Double) field.get(this);
          } else {
            fieldVal = field.get(this).toString();
          }
          field.setAccessible(accessible);
        } catch (NoSuchFieldException | IllegalAccessException e) {
          LOG.error("Failed to get default value for field {} of class {}; exception: {}", "DEFAULT_" + fieldName,
              c.getSimpleName(), e.toString());
        }
        // If failed to get the default value from Class definition, then use value 0d
        if (value == null && fieldVal == null) {
          value = 0d;
        }
        LOG.warn("Unable to read the setting for the field {} of class {}; the value {} is used.", fieldName,
            c.getSimpleName(), value);
      }
      // Set the final value to the specified field
      try {
        Field field = c.getDeclaredField(fieldName);
        boolean accessible = field.isAccessible();
        field.setAccessible(true);
        if (field.getType().equals(Double.class) || field.getType().equals(double.class)) {
          field.set(this, value);
        }
        else if (field.getType().equals(String.class)) {
            field.set(this, fieldVal);
          }
        else {
         throw new IllegalAccessException ("Field type is neither Double or String, cannot set value!");
        }
        field.setAccessible(accessible);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        LOG.warn("Failed to set the field {} for class {} exception: {}", fieldName, c.getSimpleName(), e.toString());
      }
    }
  }


  public Properties toProperties(){
    return new Properties();
  }

  /**
   * get Alert Filter Minimum Time to Detect in HOUR given severity value
   * @param severity severity of an anomaly
   * @return minimum time to detect in HOUR given severity
   */
  public double getAlertFilterMTTD (double severity) {
    return 0.0;
  }
}

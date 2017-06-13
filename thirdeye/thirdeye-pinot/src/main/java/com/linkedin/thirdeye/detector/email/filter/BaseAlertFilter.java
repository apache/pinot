package com.linkedin.thirdeye.detector.email.filter;

import java.lang.reflect.Field;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.swing.StringUIClientPropertyKey;


public abstract class BaseAlertFilter implements AlertFilter {
  private final static Logger LOG = LoggerFactory.getLogger(BaseAlertFilter.class);

//  // Check if the given string can be parsed as double value
//  public static boolean isNumeric(String str) {
//    try {
//      double d = Double.parseDouble(str);
//    }
//    catch(NumberFormatException nfe) {
//      return false;
//    }
//    return true;
//  }


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
      String strVal = null;
      // Get user's value for the specified field
      if (parameterSetting.containsKey(fieldName)) {
        String fieldVal = parameterSetting.get(fieldName);
        if (StringUtils.isNumeric(fieldVal)) {
          value = Double.parseDouble(parameterSetting.get(fieldName));
        } else {
          strVal = fieldVal;
        }
      } else {
        // If user's value does not exist, try to get the default value from Class definition
        try {
          Field field = c.getDeclaredField("DEFAULT_" + fieldName.toUpperCase());
          boolean accessible = field.isAccessible();
          field.setAccessible(true);
          Object object = field.get(this);
          if (object instanceof Double)  {
            value = (Double) object;
          } else {
            strVal = object.toString();
          }
          field.setAccessible(accessible);
        } catch (NoSuchFieldException | IllegalAccessException e) {
          LOG.error("Failed to get default value for field {} of class {}; exception: {}", "DEFAULT_" + fieldName,
              c.getSimpleName(), e.toString());
        }
        // If failed to get the default value from Class definition, then use value 0d
        if (value == null && strVal == null) {
          value = 0d;
        }
        LOG.warn("Unable to read the setting for the field {} of class {}; the value {} is used.", fieldName,
            c.getSimpleName(), value);
      }
      // Set the final value to the specified field
      try {
        Field field = c.getDeclaredField(fieldName);
        Object object = field.get(this);
        boolean accessible = field.isAccessible();
        field.setAccessible(true);
        if (object instanceof Double) {
          field.set(this, value);
        }
        else {
          field.set(this, strVal);
        }
        field.setAccessible(accessible);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        LOG.warn("Failed to set the field {} for class {} exception: {}", fieldName, c.getSimpleName(), e.toString());
      }
    }
  }
}

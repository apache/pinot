package com.linkedin.thirdeye.config;

import com.linkedin.thirdeye.datalayer.bao.ConfigManager;
import com.linkedin.thirdeye.datalayer.dto.ConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * ConfigNamespace provides safe access to ConfigBean properties by enforcing a fixed namespace.
 * It provides basic get, put, list, and delete functionality.  Use ConfigNamespace to store
 * simple key-value configuration properties that (a) require at-runtime modification of
 * configuration values, or (b) seem too trivial to justify the introduction of a separate
 * new bean class.
 */
public class ConfigNamespace {
  private final String namespace;
  private final ConfigManager configDAO;

  /**
   * Constructor for dependency injection
   *
   * @param namespace config namespace
   * @param configDAO config DAO
   */
  public ConfigNamespace(String namespace, ConfigManager configDAO) {
    this.namespace = namespace;
    this.configDAO = configDAO;
  }

  /**
   * Constructor for injection via DAORegistry
   *
   * @param namespace config namespace
   */
  public ConfigNamespace(String namespace) {
    this.namespace = namespace;
    this.configDAO = DAORegistry.getInstance().getConfigDAO();
  }

  /**
   * Returns a configuration property from storage and performs a type-cast.
   *
   * @param name property name
   * @param <T> type-cast
   * @return type-cast configuration value
   * @throws IllegalArgumentException if the key cannot be found
   */
  @SuppressWarnings("unchecked")
  public <T> T get(String name) {
    ConfigDTO result = this.configDAO.findByNamespaceName(this.namespace, name);
    if (result == null) {
      throw new IllegalArgumentException(String.format("Could not retrieve '%s':'%s'", this.namespace, name));
    }
    return (T) result.getValue();
  }

  /**
   * Returns all configuration properties within the namespace as a map (keyed by property name)
   *
   * @return map of all config key-value pairs within the namespace
   */
  public Map<String, Object> getAll() {
    Map<String, Object> output = new HashMap<>();
    List<ConfigDTO> configs = this.configDAO.findByNamespace(this.namespace);
    for (ConfigDTO config : configs) {
      output.put(config.getName(), config.getValue());
    }
    return output;
  }

  /**
   * Stores a configuration property via serialization, overwriting existing values (if any).
   *
   * @param name property name
   * @param value property value
   */
  public void put(String name, Object value) {
    ConfigDTO config = new ConfigDTO();
    config.setName(name);
    config.setNamespace(this.namespace);
    config.setValue(value);

    // NOTE: this isn't transactional, so interference is possible
    this.configDAO.deleteByNamespaceName(this.namespace, name);

    if (this.configDAO.save(config) == null) {
      throw new RuntimeException(String.format("Could not store '%s':'%s' = '%s'", this.namespace, name, value.toString()));
    }
  }

  /**
   * Deletes a configuration property from the namespace. Returns silently
   * if the property does not exist.
   *
   * @param name property name
   */
  public void delete(String name) {
    this.configDAO.deleteByNamespaceName(this.namespace, name);
  }
}

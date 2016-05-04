package com.linkedin.thirdeye.dashboard.configs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileBasedConfigDAOFactory implements ConfigDAOFactory {
  private static final Logger LOG = LoggerFactory.getLogger(FileBasedConfigDAOFactory.class);
  private static String FILE_EXT = ".json";
  private final String rootDir;
  private AbstractConfigDAO<CollectionConfig> collectionConfigDAO;
  private AbstractConfigDAO<DashboardConfig> dashboardConfigDAO;
  private AbstractConfigDAO<WidgetConfig> widgetConfigDAO;

  public FileBasedConfigDAOFactory(String rootDir) {
    this.rootDir = rootDir;
  }

  @Override
  public AbstractConfigDAO<CollectionConfig> getCollectionConfigDAO() {
    final FileBasedConfigDAO<CollectionConfig> fileBasedConfigDAO =
        new FileBasedConfigDAO<>(rootDir, CollectionConfig.class.getSimpleName());
    collectionConfigDAO = createDAO(fileBasedConfigDAO, CollectionConfig.class.getSimpleName());
    return collectionConfigDAO;
  }

  @Override
  public AbstractConfigDAO<DashboardConfig> getDashboardConfigDAO() {
    final FileBasedConfigDAO<DashboardConfig> fileBasedConfigDAO =
        new FileBasedConfigDAO<>(rootDir, DashboardConfig.class.getSimpleName());
    dashboardConfigDAO = createDAO(fileBasedConfigDAO, DashboardConfig.class.getSimpleName());

    return dashboardConfigDAO;
  }

  @Override
  public AbstractConfigDAO<WidgetConfig> getWidgetConfigDAO() {
    final FileBasedConfigDAO<WidgetConfig> fileBasedConfigDAO =
        new FileBasedConfigDAO<>(rootDir, WidgetConfig.class.getSimpleName());
    widgetConfigDAO = createDAO(fileBasedConfigDAO, WidgetConfig.class.getSimpleName());
    return widgetConfigDAO;
  }

  private <T extends AbstractConfig> AbstractConfigDAO<T> createDAO(
      final FileBasedConfigDAO<T> fileBasedConfigDAO, String configType) {
    return new AbstractConfigDAO<T>() {

      @Override
      public boolean update(String collectionName, String id, T config) throws Exception {
        return fileBasedConfigDAO.update(collectionName, id, config);
      }

      @Override
      public T findById(String collectionName, String id) {
        return fileBasedConfigDAO.findById(collectionName, id);
      }

      @SuppressWarnings("unchecked")
      @Override
      public List<T> findAll(String collectionName) {
        List<AbstractConfig> list = fileBasedConfigDAO.findAll(collectionName);
        List<T> ret = new ArrayList<>();
        for (AbstractConfig config : list) {
          ret.add((T) config);
        }
        return ret;
      }

      @Override
      public boolean create(String collectionName, String id, T config) throws Exception {
        return fileBasedConfigDAO.create(collectionName, id, config);
      }
    };
  }

  class FileBasedConfigDAO<T extends AbstractConfig> {
    private final File rootDir;
    private final String configType;
    Class<? extends AbstractConfig> configTypeClass;
    // private Constructor<T> constructor;

    public FileBasedConfigDAO(String rootDir, String configType) {
      this.rootDir = new File(rootDir);
      this.configType = configType;
      if (configType.equalsIgnoreCase("collectionConfig")) {
        configTypeClass = CollectionConfig.class;
      } else if (configType.equalsIgnoreCase("dashboardConfig")) {
        configTypeClass = DashboardConfig.class;
      } else if (configType.equalsIgnoreCase("widgetConfig")) {
        configTypeClass = WidgetConfig.class;
      } else {
        throw new IllegalArgumentException("Unknown configType:" + configType);
      }
      try {
        // constructor = configTypeClass.getConstructor();
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Error getting constructor for class :" + configTypeClass, e);
      }
    }

    public List<AbstractConfig> findAll(String collectionName) {

      File[] listFiles = getDir(collectionName).listFiles();
      if (listFiles == null || listFiles.length == 0) {
        return Collections.emptyList();
      }
      List<AbstractConfig> list = new ArrayList<>();
      for (File file : listFiles) {
        AbstractConfig instance = parseFile(file);
        list.add(instance);

      }
      return list;
    }

    private File getDir(String collectionName) {
      return new File(rootDir, collectionName + "/" + configType);
    }

    private File getFile(String collectionName, String id) {
      return new File(getDir(collectionName), id + FILE_EXT);
    }

    private T parseFile(File file) {
      InputStream input = null;
      try {
        input = new FileInputStream(file);
        String json = IOUtils.toString(input, "UTF-8");
        System.out.println("file " + file.getAbsolutePath() + " content" + json);
        T instance = AbstractConfig.fromJSON(json, configTypeClass);
        return instance;
      } catch (Exception e) {
        LOG.error("Error parsing file:" + file, e);
      } finally {
        if (input != null) {
          IOUtils.closeQuietly(input);
        }
      }
      return null;
    }

    public T findById(String collectionName, String id) {
      File file = getFile(collectionName, id);
      return parseFile(file);
    }

    public boolean create(String collectionName, String id, T config) throws Exception {
      File file = getFile(collectionName, id);
      file.getParentFile().mkdirs();
      System.out.println("Writing " + config.toJSON() + " at " + file.getAbsolutePath());
      FileWriter fileWriter = new FileWriter(file);
      IOUtils.write(config.toJSON(), fileWriter);
      fileWriter.flush();
      fileWriter.close();
      return true;
    }

    public boolean update(String collectionName, String id, AbstractConfig config)
        throws Exception {
      File file = getFile(collectionName, id);
      IOUtils.write(config.toJSON(), new FileWriter(file));
      return true;
    }
  }

}

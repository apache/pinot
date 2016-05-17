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

import com.linkedin.thirdeye.api.CollectionSchema;

public class FileBasedConfigDAOFactory implements ConfigDAOFactory {
  private static final Logger LOG = LoggerFactory.getLogger(FileBasedConfigDAOFactory.class);
  private static String FILE_EXT = ".json";
  private final String rootDir;
  private AbstractConfigDAO<CollectionConfig> collectionConfigDAO;
  private AbstractConfigDAO<DashboardConfig> dashboardConfigDAO;
  private AbstractConfigDAO<WidgetConfig> widgetConfigDAO;
  private AbstractConfigDAO<CollectionSchema> collectionSchemaDAO;

  public FileBasedConfigDAOFactory(String rootDir) {
    this.rootDir = rootDir;
  }

  @Override
  public AbstractConfigDAO<CollectionSchema> getCollectionSchemaDAO() {
    final FileBasedConfigDAO<CollectionSchema> fileBasedConfigDAO =
        new FileBasedConfigDAO<>(rootDir, CollectionSchema.class.getSimpleName());
    collectionSchemaDAO = createDAO(fileBasedConfigDAO, CollectionSchema.class.getSimpleName());
    return collectionSchemaDAO;
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
      public boolean update(String id, T config) throws Exception {
        return fileBasedConfigDAO.update(id, config);
      }

      @Override
      public T findById(String id) {
        return fileBasedConfigDAO.findById(id);
      }

      @SuppressWarnings("unchecked")
      @Override
      public List<T> findAll() {
        List<AbstractConfig> list = fileBasedConfigDAO.findAll();
        List<T> ret = new ArrayList<>();
        for (AbstractConfig config : list) {
          ret.add((T) config);
        }
        return ret;
      }

      @Override
      public boolean create(String id, T config) throws Exception {
        return fileBasedConfigDAO.create(id, config);
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
      } else if (configType.equalsIgnoreCase("collectionSchema")) {
        configTypeClass = CollectionSchema.class;
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

    public List<AbstractConfig> findAll() {

      File dir = getConfigTypeRootDir();
      File[] listFiles = dir.listFiles();
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

    private File getConfigTypeRootDir() {
      return new File(rootDir, configType);
    }

    private File getFile(String id) {
      return new File(getConfigTypeRootDir(), id + FILE_EXT);
    }

    private T parseFile(File file) {
      if (!file.exists()) {
        return null;
      }
      InputStream input = null;
      try {
        input = new FileInputStream(file);
        String json = IOUtils.toString(input, "UTF-8");
        System.out.println("file  " + file.getAbsolutePath() + " content" + json);
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

    public T findById(String id) {
      File file = getFile(id);
      T t =  parseFile(file);
      return t;
    }

    public boolean create(String id, T config) throws Exception {
      File file = getFile(id);
      file.getParentFile().mkdirs();
      System.out.println("Writing " + config.toJSON() + " at " + file.getAbsolutePath());
      FileWriter fileWriter = new FileWriter(file);
      IOUtils.write(config.toJSON(), fileWriter);
      fileWriter.flush();
      fileWriter.close();
      return true;
    }

    public boolean update(String id, AbstractConfig config) throws Exception {
      File file = getFile(id);
      IOUtils.write(config.toJSON(), new FileWriter(file));
      return true;
    }
  }

}

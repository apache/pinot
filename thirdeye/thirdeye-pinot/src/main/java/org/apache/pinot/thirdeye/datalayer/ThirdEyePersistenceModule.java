package org.apache.pinot.thirdeye.datalayer;

import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import javax.inject.Singleton;
import org.apache.pinot.thirdeye.datalayer.bao.AlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.AlertSnapshotManager;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalyFunctionManager;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalySubscriptionGroupNotificationManager;
import org.apache.pinot.thirdeye.datalayer.bao.ApplicationManager;
import org.apache.pinot.thirdeye.datalayer.bao.ClassificationConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.ConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DataCompletenessConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionStatusManager;
import org.apache.pinot.thirdeye.datalayer.bao.EntityToEntityMappingManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.GroupedAnomalyResultsManager;
import org.apache.pinot.thirdeye.datalayer.bao.JobManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.OnboardDatasetMetricManager;
import org.apache.pinot.thirdeye.datalayer.bao.OnlineDetectionDataManager;
import org.apache.pinot.thirdeye.datalayer.bao.OverrideConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.RawAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.RootcauseSessionManager;
import org.apache.pinot.thirdeye.datalayer.bao.RootcauseTemplateManager;
import org.apache.pinot.thirdeye.datalayer.bao.SessionManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.AlertConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.AlertSnapshotManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.AnomalySubscriptionGroupNotificationManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.ApplicationManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.ClassificationConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.ConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.DataCompletenessConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.DatasetConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.DetectionAlertConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.DetectionConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.DetectionStatusManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.EntityToEntityMappingManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.EvaluationManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.EventManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.GroupedAnomalyResultsManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.JobManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.MetricConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.OnboardDatasetMetricManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.OnlineDetectionDataManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.OverrideConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.RootcauseSessionManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.RootcauseTemplateManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.SessionManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.TaskManagerImpl;
import org.apache.pinot.thirdeye.datalayer.entity.AbstractEntity;
import org.apache.pinot.thirdeye.datalayer.entity.AlertConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.AlertSnapshotIndex;
import org.apache.pinot.thirdeye.datalayer.entity.AnomalyFeedbackIndex;
import org.apache.pinot.thirdeye.datalayer.entity.AnomalyFunctionIndex;
import org.apache.pinot.thirdeye.datalayer.entity.AnomalySubscriptionGroupNotificationIndex;
import org.apache.pinot.thirdeye.datalayer.entity.ApplicationIndex;
import org.apache.pinot.thirdeye.datalayer.entity.ClassificationConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.ConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.DataCompletenessConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.DatasetConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.DetectionAlertConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.DetectionConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.DetectionStatusIndex;
import org.apache.pinot.thirdeye.datalayer.entity.EntityToEntityMappingIndex;
import org.apache.pinot.thirdeye.datalayer.entity.EvaluationIndex;
import org.apache.pinot.thirdeye.datalayer.entity.EventIndex;
import org.apache.pinot.thirdeye.datalayer.entity.GenericJsonEntity;
import org.apache.pinot.thirdeye.datalayer.entity.GroupedAnomalyResultsIndex;
import org.apache.pinot.thirdeye.datalayer.entity.JobIndex;
import org.apache.pinot.thirdeye.datalayer.entity.MergedAnomalyResultIndex;
import org.apache.pinot.thirdeye.datalayer.entity.MetricConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.OnboardDatasetMetricIndex;
import org.apache.pinot.thirdeye.datalayer.entity.OnlineDetectionDataIndex;
import org.apache.pinot.thirdeye.datalayer.entity.OverrideConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.RawAnomalyResultIndex;
import org.apache.pinot.thirdeye.datalayer.entity.RootcauseSessionIndex;
import org.apache.pinot.thirdeye.datalayer.entity.RootcauseTemplateIndex;
import org.apache.pinot.thirdeye.datalayer.entity.SessionIndex;
import org.apache.pinot.thirdeye.datalayer.entity.TaskIndex;
import org.apache.pinot.thirdeye.datalayer.util.EntityMappingHolder;
import org.apache.tomcat.jdbc.pool.DataSource;

public class ThirdEyePersistenceModule extends AbstractModule {

  private static final List<Class<? extends AbstractEntity>> ENTITY_CLASSES = Arrays.asList(
      GenericJsonEntity.class,
      AnomalyFeedbackIndex.class,
      AnomalyFunctionIndex.class,
      JobIndex.class,
      MergedAnomalyResultIndex.class,
      RawAnomalyResultIndex.class,
      TaskIndex.class,
      DatasetConfigIndex.class,
      MetricConfigIndex.class,
      OverrideConfigIndex.class,
      AlertConfigIndex.class,
      DataCompletenessConfigIndex.class,
      EventIndex.class,
      DetectionStatusIndex.class,
      ClassificationConfigIndex.class,
      EntityToEntityMappingIndex.class,
      GroupedAnomalyResultsIndex.class,
      OnboardDatasetMetricIndex.class,
      ConfigIndex.class,
      ApplicationIndex.class,
      AlertSnapshotIndex.class,
      RootcauseSessionIndex.class,
      SessionIndex.class,
      DetectionConfigIndex.class,
      DetectionAlertConfigIndex.class,
      EvaluationIndex.class,
      RootcauseTemplateIndex.class,
      OnlineDetectionDataIndex.class,
      AnomalySubscriptionGroupNotificationIndex.class
  );

  private final DataSource dataSource;

  public ThirdEyePersistenceModule(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public static String camelCaseToUnderscore(String str) {
    return UPPER_CAMEL.to(LOWER_UNDERSCORE, str);
  }

  @Override
  protected void configure() {
    bind(javax.sql.DataSource.class).toInstance(dataSource);
    bind(DataSource.class).toInstance(dataSource);

    bind(AnomalyFunctionManager.class).to(AnomalyFunctionManagerImpl.class).in(Scopes.SINGLETON);
    bind(AlertConfigManager.class).to(AlertConfigManagerImpl.class).in(Scopes.SINGLETON);
    bind(RawAnomalyResultManager.class).to(RawAnomalyResultManagerImpl.class).in(Scopes.SINGLETON);
    bind(MergedAnomalyResultManager.class).to(MergedAnomalyResultManagerImpl.class).in(
        Scopes.SINGLETON);
    bind(JobManager.class).to(JobManagerImpl.class).in(Scopes.SINGLETON);
    bind(TaskManager.class).to(TaskManagerImpl.class).in(Scopes.SINGLETON);
    bind(DatasetConfigManager.class).to(DatasetConfigManagerImpl.class).in(Scopes.SINGLETON);
    bind(MetricConfigManager.class).to(MetricConfigManagerImpl.class).in(Scopes.SINGLETON);
    bind(OverrideConfigManager.class).to(OverrideConfigManagerImpl.class).in(Scopes.SINGLETON);
    bind(DataCompletenessConfigManager.class).to(DataCompletenessConfigManagerImpl.class).in(
        Scopes.SINGLETON);
    bind(EventManager.class).to(EventManagerImpl.class).in(Scopes.SINGLETON);
    bind(DetectionStatusManager.class).to(DetectionStatusManagerImpl.class).in(Scopes.SINGLETON);
    bind(ClassificationConfigManager.class).to(ClassificationConfigManagerImpl.class).in(
        Scopes.SINGLETON);
    bind(EntityToEntityMappingManager.class).to(EntityToEntityMappingManagerImpl.class)
        .in(Scopes.SINGLETON);
    bind(GroupedAnomalyResultsManager.class).to(GroupedAnomalyResultsManagerImpl.class)
        .in(Scopes.SINGLETON);
    bind(OnboardDatasetMetricManager.class).to(OnboardDatasetMetricManagerImpl.class).in(
        Scopes.SINGLETON);
    bind(ConfigManager.class).to(ConfigManagerImpl.class).in(Scopes.SINGLETON);
    bind(ApplicationManager.class).to(ApplicationManagerImpl.class).in(Scopes.SINGLETON);
    bind(AlertSnapshotManager.class).to(AlertSnapshotManagerImpl.class).in(Scopes.SINGLETON);
    bind(RootcauseSessionManager.class).to(RootcauseSessionManagerImpl.class).in(Scopes.SINGLETON);
    bind(RootcauseTemplateManager.class).to(RootcauseTemplateManagerImpl.class)
        .in(Scopes.SINGLETON);
    bind(SessionManager.class).to(SessionManagerImpl.class).in(Scopes.SINGLETON);
    bind(DetectionConfigManager.class).to(DetectionConfigManagerImpl.class).in(Scopes.SINGLETON);
    bind(DetectionAlertConfigManager.class).to(DetectionAlertConfigManagerImpl.class).in(
        Scopes.SINGLETON);
    bind(EvaluationManager.class).to(EvaluationManagerImpl.class).in(Scopes.SINGLETON);
    bind(OnlineDetectionDataManager.class).to(OnlineDetectionDataManagerImpl.class).in(
        Scopes.SINGLETON);
    bind(AnomalySubscriptionGroupNotificationManager.class)
        .to(AnomalySubscriptionGroupNotificationManagerImpl.class)
        .in(Scopes.SINGLETON);
  }

  @Singleton
  @Provides
  public EntityMappingHolder getEntityMappingHolder(final DataSource dataSource) {
    final EntityMappingHolder entityMappingHolder = new EntityMappingHolder();
    try (Connection connection = dataSource.getConnection()) {
      for (Class<? extends AbstractEntity> clazz : ENTITY_CLASSES) {
        final String tableName = camelCaseToUnderscore(clazz.getSimpleName());
        entityMappingHolder.register(connection, clazz, tableName);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return entityMappingHolder;
  }
}

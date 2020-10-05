package org.apache.pinot.thirdeye.datalayer;

import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import javax.inject.Singleton;
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
  }

  @Singleton
  @Provides
  public EntityMappingHolder getEntityMappingHolder(final DataSource dataSource) {
    final EntityMappingHolder entityMappingHolder;
    entityMappingHolder = new EntityMappingHolder();
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

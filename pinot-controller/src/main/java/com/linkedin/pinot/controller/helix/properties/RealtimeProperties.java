package com.linkedin.pinot.controller.helix.properties;

import java.util.ArrayList;
import java.util.List;


public class RealtimeProperties {
  /*
   * schema realted configs are not included here, since that is something we will have to construct
   * ourselves
   */
  public enum RESOURCE_LEVEL {
    pinot_cluster_type("pinot.cluster.type"),
    //sensei_conf_path("sensei.conf.path"),
    httpServer_numReplicas("httpServer.numReplicas"),
    sensei_cluster_name("sensei.cluster.name"),
    kafka_topic_name("pinot.realtime.kafka.topic.name"),
    indexingCoordinator_sortedColumns("indexingCoordinator.sortedColumns"),
    indexingCoordinator_shardedColumn("indexingCoordinator.shardedColumn"),
    indexingCoordinator_retention_timeUnit("indexingCoordinator.retention.timeUnit"),
    indexingCoordinator_retention_duration("indexingCoordinator.retention.duration"),
    indexingCoordinator_retention_latency_time_column("indexingCoordinator.retention.latency.time.column"),
    sensei_index_manager_default_maxpartition_id("sensei.index.manager.default.maxpartition.id");
    //dataProvider_instance("dataProvider.instance");

    private final String name;

    private RESOURCE_LEVEL(String name) {
      this.name = name;
    }

    public List<String> getKeys() {
      List<String> resourceConfigKeys = new ArrayList<String>();
      for (RESOURCE_LEVEL configKey : values()) {
        resourceConfigKeys.add(configKey.name);
      }
      return resourceConfigKeys;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  public enum INSTANCE_LEVEL {
    sensei_node_id("sensei.node.id"),
    sensei_node_partitions("sensei.node.partitions");

    private final String name;

    private INSTANCE_LEVEL(String name) {
      this.name = name;
    }

    public List<String> getKeys() {
      List<String> resourceConfigKeys = new ArrayList<String>();
      for (INSTANCE_LEVEL configKey : values()) {
        resourceConfigKeys.add(configKey.name);
      }
      return resourceConfigKeys;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  public enum DEFAULT_LEVEL {
    sensei_cluster_url("sensei.cluster.url"),
    sensei_index_directory("sensei.index.directory"),
    indexingCoordinator_indexDir("indexingCoordinator.indexDir"),
    sensei_node_isAvailable("sensei.node.isAvailable"),
    sensei_server_port("sensei.server.port"),
    sensei_server_requestThreadCorePoolSize("sensei.server.requestThreadCorePoolSize"),
    sensei_server_requestThreadMaxPoolSize("sensei.server.requestThreadMaxPoolSize"),
    sensei_server_requestThreadKeepAliveTimeSecs("sensei.server.requestThreadKeepAliveTimeSecs"),
    sensei_cluster_timeout("sensei.cluster.timeout"),
    sensei_indexer_type("sensei.indexer.type"),
    indexingCoordinator_class("indexingCoordinator.class"),
    indexingCoordinator_dataProvider("indexingCoordinator.dataProvider"),
    indexingCoordinator_clusterName("indexingCoordinator.clusterName"),
    indexingCoordinator_numServingPartitions("indexingCoordinator.numServingPartitions"),
    indexingCoordinator_capacity("indexingCoordinator.capacity"),
    indexingCoordinator_refreshTime("indexingCoordinator.refreshTime"),
    indexingCoordinator_bufferSize("indexingCoordinator.bufferSize"),
    indexingCoordinator_readMode("indexingCoordinator.readMode"),
    indexingCoordinator_schema("indexingCoordinator.schema"),
    sensei_query_builder_factory_class("sensei.query.builder.factory.class"),
    sensei_index_interpreter_class("sensei.index.interpreter.class"),
    sensei_index_pruner_class("sensei.index.pruner.class"),
    sensei_mapreduce_accessor_factory_class("sensei.mapreduce.accessor.factory.class"),
    sensei_request_postrocessor_class("sensei.request.postrocessor.class"),
    sensei_index_manager_class("sensei.index.manager.class"),
    realtime_core_service_class("realtime.core.service.class"),
    realtime_sys_service_class("realtime.sys.service.class"),
    sensei_plugin_services_list("sensei.plugin.services.list"),
    sensei_search_pluggableEngines_list("sensei.search.pluggableEngines.list"),
    sensei_plugin_services("sensei.plugin.services"),
    sensei_broker_host("sensei.broker.host"),
    sensei_broker_port("sensei.broker.port"),
    sensei_broker_minThread("sensei.broker.minThread"),
    sensei_broker_maxThread("sensei.broker.maxThread"),
    sensei_broker_maxWaittime("sensei.broker.maxWaittime"),
    sensei_broker_webapp_path("sensei.broker.webapp.path"),
    sensei_broker_timeout("sensei.broker.timeout"),
    sensei_partition_timeout("sensei.partition.timeout"),
    sensei_broker_allowPartialMerge("sensei.broker.allowPartialMerge"),
    sensei_broker_metrics_updateTotalCount("sensei.broker.metrics.updateTotalCount"),
    sensei_search_cluster_name("sensei.search.cluster.name"),
    sensei_search_cluster_zookeeper_url("sensei.search.cluster.zookeeper.url"),
    sensei_search_cluster_zookeeper_conn_timeout("sensei.search.cluster.zookeeper.conn.timeout"),
    sensei_search_cluster_network_conn_timeout("sensei.search.cluster.network.conn.timeout"),
    sensei_search_cluster_network_write_timeout("sensei.search.cluster.network.write.timeout"),
    sensei_search_cluster_network_max_conn_per_node("sensei.search.cluster.network.max.conn.per.node"),
    sensei_search_cluster_network_stale_timeout_mins("sensei.search.cluster.network.stale.timeout.mins"),
    sensei_search_cluster_network_stale_cleanup_freq_mins("sensei.search.cluster.network.stale.cleanup.freq.mins"),
    sensei_federated_broker_class("sensei.federated.broker.class"),
    sensei_custom_facets_list("sensei.custom.facets.list");

    private final String name;

    private DEFAULT_LEVEL(String name) {
      this.name = name;
    }

    public List<String> getKeys() {
      List<String> resourceConfigKeys = new ArrayList<String>();
      for (DEFAULT_LEVEL configKey : values()) {
        resourceConfigKeys.add(configKey.name);
      }
      return resourceConfigKeys;
    }

    @Override
    public String toString() {
      return name;
    }
  }
}

package com.linkedin.pinot.controller.helix.properties;

import java.util.ArrayList;
import java.util.List;


public class OfflineProperties {

  public enum RESOURCE_LEVEL {
    pinot_resource_type("pinot.resource.type"),
    pinot_resource_name("pinot.resource.name"),
    pinot_resource_numReplicas("pinot.resource.numReplicas"),
    pinot_resource_numInstancesPerReplica("pinot.resource.numInstancesPerReplica"),
    pinot_offline_retention_time_unit("pinot.controller.deletion.time.unit"),
    pinot_offline_retention_time_column("pinot.controller.deletion.time.column"),
    pinot_offline_retention_duration("pinot.controller.deletion.duration");

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
    httpServer_directory("httpServer.directory"),
    sensei_search_cluster_zookeeper_url("sensei.search.cluster.zookeeper.url"),
    sensei_node_isAvailable("sensei.node.isAvailable"),
    sensei_conf_path("sensei.conf.path"),
    sensei_server_port("sensei.server.port"),
    sensei_server_requestThreadCorePoolSize("sensei.server.requestThreadCorePoolSize"),
    sensei_server_requestThreadMaxPoolSize("sensei.server.requestThreadMaxPoolSize"),
    sensei_server_requestThreadKeepAliveTimeSecs("sensei.server.requestThreadKeepAliveTimeSecs"),
    sensei_cluster_timeout("sensei.cluster.timeout"),
    ba_index_factory_readMode("ba.index.factory.readMode"),
    ba_index_factory_convertToBitmap("ba.index.factory.convertToBitmap"),
    ba_index_factory_maxDictSizeForBitmap("ba.index.factory.maxDictSizeForBitmap"),
    ba_index_factory_invertedColumns("ba.index.factory.invertedColumns"),
    ba_index_factory_memoryMappedColumns("ba.index.factory.memoryMappedColumns"),
    ba_index_factory_excludeColumns("ba.index.factory.excludeColumns"),
    sensei_indexer_type("sensei.indexer.type"),
    ba_index_factory_class("ba.index.factory.class"),
    sensei_query_builder_factory_class("sensei.query.builder.factory.class"),
    sensei_index_interpreter_class("sensei.index.interpreter.class"),
    sensei_index_pruner_class("sensei.index.pruner.class"),
    sensei_mapreduce_accessor_factory_class("sensei.mapreduce.accessor.factory.class"),
    sensei_request_postrocessor_class("sensei.request.postrocessor.class"),
    httpServer_class("httpServer.class"),
    httpServer_port("httpServer.port"),
    sensei_plugin_services("sensei.plugin.services"),
    sensei_broker_host("sensei.broker.host"),
    sensei_broker_port("sensei.broker.port"),
    sensei_broker_minThread("sensei.broker.minThread"),
    sensei_broker_maxThread("sensei.broker.maxThread"),
    sensei_broker_maxWaittime("sensei.broker.maxWaittime"),
    sensei_broker_timeout("sensei.broker.timeout"),
    sensei_broker_allowPartialMerge("sensei.broker.allowPartialMerge"),
    sensei_partition_timeout("sensei.partition.timeout"),
    sensei_broker_metrics_updateTotalCount("sensei.broker.metrics.updateTotalCount"),
    sensei_broker_webapp_path("sensei.broker.webapp.path"),
    sensei_search_cluster_name("sensei.search.cluster.name"),
    sensei_search_cluster_zookeeper_conn_timeout("sensei.search.cluster.zookeeper.conn.timeout"),
    sensei_search_cluster_network_conn_timeout("sensei.search.cluster.network.conn.timeout"),
    sensei_search_cluster_network_write_timeout("sensei.search.cluster.network.write.timeout"),
    sensei_search_cluster_network_max_conn_per_node("sensei.search.cluster.network.max.conn.per.node"),
    sensei_search_cluster_network_stale_timeout_mins("sensei.search.cluster.network.stale.timeout.mins"),
    sensei_search_cluster_network_stale_cleanup_freq_mins("sensei.search.cluster.network.stale.cleanup.freq.mins"),
    sensei_custom_facets_list("sensei.custom.facets.list"),
    sensei_federated_broker_class("sensei.federated.broker.class"), ;
    private final String name;

    private DEFAULT_LEVEL(String name) {
      this.name = name;
    }

    public static List<String> getKeys() {
      List<String> resourceConfigKeys = new ArrayList<String>();
      for (DEFAULT_LEVEL configKey : values()) {
        resourceConfigKeys.add(configKey.name);
      }
      return resourceConfigKeys;
    }

    public static boolean hasKey(String key) {
      for (DEFAULT_LEVEL level : values()) {
        if (level.name.equals(key)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public String toString() {
      return name;
    }
  }
}

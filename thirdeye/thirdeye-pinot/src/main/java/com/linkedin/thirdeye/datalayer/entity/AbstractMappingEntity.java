package com.linkedin.thirdeye.datalayer.entity;

import java.util.List;

public abstract class AbstractMappingEntity<P extends AbstractJsonEntity, Q extends AbstractJsonEntity> {
  List<P> ps;
  List<Q> qs;

  protected AbstractMappingEntity(List<Long> pIds, List<Long> qIds) {
    // TODO : populate mapped entities, Table and column names can be derived from Class name,
    // and P/Q class types

    // query : select pColumnName, qColumnName from mappingTable where pColumnName in :pIds
    // qColumnName in :qIds
    // Then fetch the entities and set in the list
  }
}

package com.linkedin.thirdeye.dbi.entity.base;

import java.util.List;

public abstract class AbstractMappingEntity<P extends AbstractEntity, Q extends AbstractEntity> {
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

package org.apache.pinot.query.catalog;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.pinot.common.utils.helix.TableCache;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Simple Catalog that only contains list of tables. Backed by {@link TableCache}.
 */
public class PinotCatalog implements Schema {

  private final TableCache _tableCache;

  public PinotCatalog(TableCache tableCache) {
    _tableCache = tableCache;
  }

  @Override
  public Table getTable(String name) {
    String tableName = TableNameBuilder.extractRawTableName(name);
    return new PinotTable(_tableCache.getSchema(tableName));
  }

  @Override
  public Set<String> getTableNames() {
    return Collections.emptySet();
  }

  @Override
  public RelProtoDataType getType(String name) {
    return null;
  }

  @Override
  public Set<String> getTypeNames() {
    return Collections.emptySet();
  }

  @Override
  public Collection<Function> getFunctions(String name) {
    return Collections.emptyList();
  }

  @Override
  public Set<String> getFunctionNames() {
    return Collections.emptySet();
  }

  @Override
  public Schema getSubSchema(String name) {
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Collections.emptySet();
  }

  @Override
  public Expression getExpression(SchemaPlus parentSchema, String name) {
    return null;
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public Schema snapshot(SchemaVersion version) {
    return this;
  }
}

package org.apache.pinot.spi.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.Objects;
import org.apache.pinot.spi.config.table.IndexingConfig;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "mode")
@JsonSubTypes({
    @JsonSubTypes.Type(value = NullHandling.TableBased.class, name = "table"),
    @JsonSubTypes.Type(value = NullHandling.ColumnBased.class, name = "column")
})
public abstract class NullHandling {

  public abstract boolean isNullable(FieldSpec spec);

  public abstract boolean supportsV2();

  /**
   * This null handling mode indicatesthat nullability is defined by {@link IndexingConfig#isNullHandlingEnabled()}.
   *
   * For compatibility reasons this is the default mode, but it acts as all or nothing and it is recommended to
   * migrate to {@link ColumnBased}, which is more versatile.
   */
  @JsonTypeName("table")
  public static class TableBased extends NullHandling {
    private static final TableBased INSTANCE = new TableBased();

    private TableBased() {
    }

    @JsonCreator
    public static TableBased getInstance() {
      return INSTANCE;
    }

    @Override
    public boolean isNullable(FieldSpec spec) {
      return true;
    }

    @Override
    public boolean supportsV2() {
      return false;
    }

    @Override
    public String toString() {
      return "{\"mode\": \"table\"}";
    }
  }

  /**
   * This null handling mode indicates that nullability is defined by {@link FieldSpec#isNullable()}.
   *
   * It is important to note that multi-stage engine supports null handling if and only if this mode is enabled for
   * all the schemas that participate in the query.
   */
  @JsonTypeName("column")
  public static class ColumnBased extends NullHandling {
    private boolean _defaultValue = false;

    @JsonCreator
    public ColumnBased() {
    }

    public ColumnBased(boolean defaultValue) {
      _defaultValue = defaultValue;
    }

    @JsonIgnore
    @Override
    public boolean supportsV2() {
      return true;
    }

    @JsonProperty("default")
    public boolean isDefaultValue() {
      return _defaultValue;
    }

    public void setDefaultValue(boolean defaultValue) {
      _defaultValue = defaultValue;
    }

    @Override
    public boolean isNullable(FieldSpec spec) {
      return spec.isNullable() == Boolean.TRUE || spec.isNullable() == null && _defaultValue;
    }

    @Override
    public String toString() {
      return "{\"mode\": \"column\", \"default\": " + _defaultValue + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ColumnBased that = (ColumnBased) o;
      return _defaultValue == that._defaultValue;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_defaultValue);
    }
  }
}

package org.apache.arrow.datafusion;

import java.util.function.Consumer;
import java.util.function.LongConsumer;

/** Configuration for creating a {@link SessionContext} using {@link SessionContexts#withConfig} */
public class SessionConfig extends AbstractProxy implements AutoCloseable {
  /** Create a new default {@link SessionConfig} */
  public SessionConfig() {
    super(create());
  }

  /**
   * Get options specific to reading Parquet data
   *
   * @return {@link ParquetOptions} instance for this config
   */
  public ParquetOptions parquetOptions() {
    return new ParquetOptions(this);
  }

  /**
   * Get options specific to parsing SQL queries
   *
   * @return {@link SqlParserOptions} instance for this config
   */
  public SqlParserOptions sqlParserOptions() {
    return new SqlParserOptions(this);
  }

  /**
   * Modify this session configuration and then return it, to simplify use in a try-with-resources
   * statement
   *
   * @param configurationCallback Callback used to update the configuration
   * @return This {@link SessionConfig} instance after being updated
   */
  public SessionConfig withConfiguration(Consumer<SessionConfig> configurationCallback) {
    configurationCallback.accept(this);
    return this;
  }

  @Override
  void doClose(long pointer) {
    destroy(pointer);
  }

  private static native long create();

  private static native void destroy(long pointer);

  // ParquetOptions native methods

  static native boolean getParquetOptionsEnablePageIndex(long pointer);

  static native void setParquetOptionsEnablePageIndex(long pointer, boolean enabled);

  static native boolean getParquetOptionsPruning(long pointer);

  static native void setParquetOptionsPruning(long pointer, boolean enabled);

  static native boolean getParquetOptionsSkipMetadata(long pointer);

  static native void setParquetOptionsSkipMetadata(long pointer, boolean enabled);

  static native void getParquetOptionsMetadataSizeHint(long pointer, LongConsumer onValue);

  static native void setParquetOptionsMetadataSizeHint(long pointer, boolean hasValue, long value);

  static native boolean getParquetOptionsPushdownFilters(long pointer);

  static native void setParquetOptionsPushdownFilters(long pointer, boolean enabled);

  static native boolean getParquetOptionsReorderFilters(long pointer);

  static native void setParquetOptionsReorderFilters(long pointer, boolean enabled);

  // SqlParserOptions native methods

  static native boolean getSqlParserOptionsParseFloatAsDecimal(long pointer);

  static native void setSqlParserOptionsParseFloatAsDecimal(long pointer, boolean enabled);

  static native boolean getSqlParserOptionsEnableIdentNormalization(long pointer);

  static native void setSqlParserOptionsEnableIdentNormalization(long pointer, boolean enabled);

  static native String getSqlParserOptionsDialect(long pointer);

  static native void setSqlParserOptionsDialect(long pointer, String dialect);
}

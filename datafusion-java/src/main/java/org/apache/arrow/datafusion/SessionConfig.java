package org.apache.arrow.datafusion;

import java.util.function.Consumer;

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

  void setParquetOptionsEnablePageIndex(boolean enabled) {
    setParquetOptionsEnablePageIndex(getPointer(), enabled);
  }

  @Override
  void doClose(long pointer) {
    destroy(pointer);
  }

  private static native long create();

  private static native void destroy(long pointer);

  private static native void setParquetOptionsEnablePageIndex(long pointer, boolean enabled);
}

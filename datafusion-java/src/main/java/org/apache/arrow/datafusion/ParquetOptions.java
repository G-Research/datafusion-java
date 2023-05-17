package org.apache.arrow.datafusion;

/** Configures options specific to reading Parquet data */
@SuppressWarnings("UnusedReturnValue")
public class ParquetOptions {
  private final SessionConfig config;

  ParquetOptions(SessionConfig config) {
    this.config = config;
  }

  /**
   * Set whether to use parquet data page level metadata (Page Index) statistics to reduce the
   * number of rows decoded.
   *
   * @param enabled whether using the page index is enabled
   * @return the modified {@link ParquetOptions} instance
   */
  public ParquetOptions withEnablePageIndex(boolean enabled) {
    config.setParquetOptionsEnablePageIndex(enabled);
    return this;
  }
}

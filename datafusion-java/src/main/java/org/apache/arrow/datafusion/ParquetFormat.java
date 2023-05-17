package org.apache.arrow.datafusion;

/** The Apache Parquet file format configuration */
public class ParquetFormat extends AbstractProxy implements FileFormat {
  /** Create new ParquetFormat with default options */
  public ParquetFormat() {
    super(create());
  }

  @Override
  void doClose(long pointer) {
    destroy(pointer);
  }

  static {
    JNILoader.load();
  }

  private static native long create();

  private static native void destroy(long pointer);
}

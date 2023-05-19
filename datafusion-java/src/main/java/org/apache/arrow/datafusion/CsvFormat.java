package org.apache.arrow.datafusion;

/** The CSV file format configuration */
public class CsvFormat extends AbstractProxy implements FileFormat {
  /** Create new CSV format with default options */
  public CsvFormat() {
    super(create());
  }

  @Override
  void doClose(long pointer) {
    destroy(pointer);
  }

  private static native long create();

  private static native void destroy(long pointer);
}

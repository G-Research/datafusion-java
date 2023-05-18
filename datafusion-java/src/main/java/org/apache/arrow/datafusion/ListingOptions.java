package org.apache.arrow.datafusion;

/** Configures options for a {@link ListingTable} */
public class ListingOptions extends AbstractProxy implements AutoCloseable {
  /** A Builder for {@link ListingOptions} instances */
  public static class Builder {
    private final FileFormat format;
    private String fileExtension = "";

    /**
     * Create a new {@link ListingOptions} builder
     *
     * @param format The file format used by data files in the listing table
     */
    public Builder(FileFormat format) {
      this.format = format;
    }

    /**
     * Specify a suffix used to filter files in the listing location
     *
     * @param fileExtension The file suffix to filter on
     * @return This builder
     */
    public Builder withFileExtension(String fileExtension) {
      this.fileExtension = fileExtension;
      return this;
    }

    /**
     * Build a new {@link ListingOptions} instance from the configured builder
     *
     * @return The built {@link ListingOptions}
     */
    public ListingOptions build() {
      return new ListingOptions(this);
    }
  }

  /**
   * Create a builder for listing options
   *
   * @param format The file format used by data files in the listing table
   * @return A new {@link Builder} instance
   */
  public static Builder builder(FileFormat format) {
    return new Builder(format);
  }

  /**
   * Construct ListingOptions from a Builder
   *
   * @param builder The builder to use
   */
  private ListingOptions(Builder builder) {
    super(create(builder.format.getPointer(), builder.fileExtension));
  }

  @Override
  void doClose(long pointer) {
    destroy(pointer);
  }

  private static native long create(long format, String fileExtension);

  private static native void destroy(long pointer);
}

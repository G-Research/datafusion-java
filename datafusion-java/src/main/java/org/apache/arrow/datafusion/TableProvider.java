package org.apache.arrow.datafusion;

/** Interface for data sources that can provide tabular data */
public interface TableProvider extends AutoCloseable, NativeProxy {}

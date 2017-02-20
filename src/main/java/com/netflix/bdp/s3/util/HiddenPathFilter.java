package com.netflix.bdp.s3.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class HiddenPathFilter implements PathFilter {
  private static final HiddenPathFilter INSTANCE = new HiddenPathFilter();

  public static HiddenPathFilter get() {
    return INSTANCE;
  }

  private HiddenPathFilter() {
  }

  @Override
  public boolean accept(Path path) {
    return (
        !path.getName().startsWith(".") &&
        !path.getName().startsWith("_")
    );
  }
}

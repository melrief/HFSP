package org.apache.hadoop.mapred;

import org.apache.log4j.Layout;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;

public class MockModeSimpleLayout extends Layout {

  public final static String LINESEP = System.getProperty("line.separator");

  public static Clock clock = new Clock();

  @Override
  public String format(LoggingEvent event) {
    return Long.toString(clock.getTime()) + " " + event.getMessage() + LINESEP;
  }

  @Override
  public void activateOptions() {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean ignoresThrowable() {
    // TODO Auto-generated method stub
    return false;
  }

}

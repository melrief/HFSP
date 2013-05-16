/* 
 * Copyright 2012 Eurecom (http://www.eurecom.fr)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import org.apache.log4j.Layout;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;

public class MockModeLayout extends Layout {

  public final static String LINESEP = System.getProperty("line.separator");

  public static Clock clock = new Clock();

  private LocationInfo li;

  @Override
  public String format(LoggingEvent event) {
    li = event.getLocationInformation();
    return clock.getTime() + " " + event.getLevel() + " " + li.getClassName()
        + "#" + li.getMethodName() + "(" + li.getLineNumber() + ")" + " "
        + event.getMessage() + LINESEP;
  }

  @Override
  public void activateOptions() {
  }

  @Override
  public boolean ignoresThrowable() {
    return false;
  }

}

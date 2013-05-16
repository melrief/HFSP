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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TaskDurationInfo extends TaskDurationInfoBase {

  private long duration;
  private final long totalDuration;

  private static final Log LOG = LogFactory.getLog(TaskDurationInfo.class);

  public TaskDurationInfo(final TaskID taskID, final long duration) {
    super(taskID);
    assert duration >= 0;

    LOG.debug("\tcreate task duration of " + duration + " for task " + taskID);
    this.totalDuration = duration;
    this.duration = duration;
  }

  public final long getDuration() {
    return duration;
  }

  public final long decrease(final long interval) {
    this.duration = (interval > this.duration) ? 0 : this.duration - interval;
    return this.duration;
  }

  @Override
  public final boolean isFinished() {
    return duration <= 0;
  }

  public final long getTotalDuration() {
    return totalDuration;
  }

}

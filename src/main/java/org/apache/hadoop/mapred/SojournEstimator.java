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

public class SojournEstimator {

  // FIXME: fix this method with something smarter and more consistent
  public long getSojournTime(TaskInProgress task, long time) {
    long finishTime = 0;
    if (task.isMapTask()) {
      finishTime = task.getExecFinishTime();
      return (finishTime == 0) ? time - task.getExecStartTime() : task
          .getExecFinishTime() - task.getExecStartTime();
    } else {
      long sortFinishTime = 0;
      for (TaskStatus status : task.getTaskStatuses()) {
        sortFinishTime = status.getSortFinishTime();
        if (sortFinishTime == 0)
          continue;
        finishTime = status.getFinishTime();
        // if (finishTime == 0 || finishTime == sortFinishTime)
        if (finishTime == 0) {
          SojournTrainer.LOG.debug(task.getTIPId() + " time: " + time
              + " sortFinishTime: " + sortFinishTime + " finishTime: 0");
          return time - sortFinishTime;
        }
        SojournTrainer.LOG.debug(task.getTIPId() + " time: " + time
            + " sortFinishTime: " + sortFinishTime + " finishTime: "
            + finishTime);
        return finishTime - sortFinishTime;
      }
    }
    return 0;
  }

}
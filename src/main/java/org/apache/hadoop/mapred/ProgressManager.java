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

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskType;

public class ProgressManager extends
    VirtualProgressManager<JobDurationInfo, TaskDurationInfo, Interval> {

  // normal log
  private static final Log LOG = LogFactory.getLog(ProgressManager.class);

  // job progress logger
  private static final Log JP_LOG = LogFactory.getLog(ProgressManager.class
      .getCanonicalName() + ".JobProgress");

  // task progress logger
  private static final Log TP_LOG = LogFactory.getLog(ProgressManager.class
      .getCanonicalName() + ".TaskProgress");

  public ProgressManager(
      final VirtualCluster<TaskDurationInfo> virtualCluster,
      final IVirtualScheduler<JobDurationInfo, TaskDurationInfo> virtualScheduler) {
    super(virtualCluster, virtualScheduler);
  }

  @Override
  public final void update(final Interval interval) {
    final long value = interval.getInterval();
    long currValue = value;
    TaskID tid = null;

    // search for interval less then the one provided as argument
    // TODO: make this more efficient by keeping the smallest duration
    // somewhere
    for (TaskType type : TaskType.values()) {
      for (TaskDurationInfo task : getCluster().getSlots(type)) {
        if (task != null) {
          long currDuration = task.getDuration();
          if (currDuration < currValue) {
            currValue = currDuration;
            tid = task.getTaskID();
          }
        }
      }
    }

    // if there is an interval less than the one provided then
    // divide the interval provided in two
    if (currValue < value) {
      Interval interval1 = new Interval(currValue);
      Interval interval2 = new Interval(value - currValue);
      LOG.debug("interval " + value + " is bigger than the finish time "
          + currValue + " of task " + tid + ". Dividing in two "
          + "intervals of respectively " + interval1.getInterval() + " and "
          + interval2.getInterval());
      this.update(interval1);
      this.update(interval2);

      // else decrement duration of each assigned task
    } else {
      boolean mustScheduleMaps = false;
      boolean mustScheduleReduces = false;
      boolean logMapProgress = false;
      boolean logReduceProgress = false;

      for (TaskType type : HFSPScheduler.TASK_TYPES) {
        for (int i = 0; i < getCluster().getSlots(type).size(); i++) {
          TaskDurationInfo task = getCluster().getSlots(type).get(i);
          if (task != null) {

            task.decrease(interval.getInterval());
            TP_LOG.info(task.getTaskID() + " " + task.getDuration());

            if (type == TaskType.MAP) {
              logMapProgress = true;
            } else {
              logReduceProgress = true;
            }

            if (task.isFinished()) {
              getCluster().freeSlot(type, i);
              if (type == TaskType.MAP) {
                mustScheduleMaps = true;
              } else {
                mustScheduleReduces = true;
              }

            }
          }
        }
      }

      this.logJobsProgress(logMapProgress, logReduceProgress);

      for (TaskType type : HFSPScheduler.TASK_TYPES) {
        if (type == TaskType.MAP ? mustScheduleMaps : mustScheduleReduces) {

          for (JobDurationInfo jdi : this.getDurationInfos(type)) {
            if (jdi.isFinished() && !jdi.hasFinishPosition()) {
              JobDurationInfo.setFinishPosition(jdi);
            }
          }

          this.getScheduler().schedule(type, getDurationInfos(type),
              getCluster());
        }
      }
      //
      // if (mustScheduleMaps) {
      // Set<JobDurationInfo> jdis = this.getDurationInfos(TaskType.MAP);
      // for (JobDurationInfo jdi : jdis) {
      // if (jdi.isFinished() && !jdi.hasFinishPosition()) {
      // JobDurationInfo.setFinishPosition(jdi);
      // }
      // }
      // this.getScheduler().schedule(TaskType.MAP,
      // getDurationInfos(TaskType.MAP),
      // getCluster());
      // }
      // if (mustScheduleReduces) {
      // Set<JobDurationInfo> jdis = this.getDurationInfos(TaskType.REDUCE);
      // for (JobDurationInfo jdi : jdis) {
      // if (jdi.isFinished() && !jdi.hasFinishPosition()) {
      // JobDurationInfo.setFinishPosition(jdi);
      // }
      // }
      // this.getScheduler().schedule(TaskType.REDUCE,
      // getDurationInfos(TaskType.REDUCE),
      // getCluster());
      // }
    }
  }

  private void logJobsProgress(boolean shouldLogMapProgress,
      boolean shouldLogReduceProgress) {
    if (JP_LOG.isDebugEnabled()) {
      if (shouldLogMapProgress) {
        this.logJobDurations(TaskType.MAP);
      }
      if (shouldLogReduceProgress) {
        this.logJobDurations(TaskType.REDUCE);
      }
    }
  }

  private void logJobDurations(TaskType type) {
    // LOG.debug("Virtual tasks progress: " + durations);
    Set<JobDurationInfo> jdis = this.getDurationInfos(type);
    for (JobDurationInfo jdi : jdis) {
      JP_LOG.info(jdi.getJobID() + ":" + type + " " + jdi.getPhaseDuration());
    }
  }

}

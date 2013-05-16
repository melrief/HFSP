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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.mapreduce.TaskType;

public class JobDurationInfo extends JobDurationInfoBase<TaskDurationInfo> {

  public static int currentMapFinishPos = 0;
  public static int currentReduceFinishPos = 0;
  private int finishPosition;

  public static final Log LOG = LogFactory.getLog(JobDurationInfo.class);

  public JobDurationInfo(final JobID jobID,
      final Map<TaskID, TaskDurationInfo> tasks, final TaskType type) {
    super(jobID, tasks, type);
    this.finishPosition = 0;
  }

  public static JobDurationInfo fromJobInProgress(final JobInProgress job,
      final TaskType type) {
    if (type == TaskType.MAP) {
      return JobDurationInfo.fromJobInProgressMapPhase(job);
    } else {
      return JobDurationInfo.fromJobInProgressReducePhase(job);
    }
  }

  @Deprecated
  public static final JobDurationInfo fromJobInProgressMapPhase(
      final JobInProgress job) {
    Map<TaskID, TaskDurationInfo> tasks = new TreeMap<TaskID, TaskDurationInfo>();
    // TODO: check tasks not initialized
    // for (TaskInProgress task : job.getTasks(TaskType.MAP)) {
    // duration = task.getMapInputSize();
    //
    // tasks.put(task.getTIPId(), new TaskDurationInfo(task.getTIPId(),
    // duration));
    // }

    long length = job.getInputLength();

    LOG.debug("create duration for job: " + job.getJobID()

    + " type: MAP" + " len(input): " + length + " tasks(" + job.numMapTasks
        + "):");
    if (job.numMapTasks > 0) {
      length /= job.numMapTasks;
    }
    for (int i = 0; i < job.numMapTasks; i++) {
      TaskID tid = new TaskID(job.getJobID(), true, i);
      tasks.put(tid, new TaskDurationInfo(tid, length));
      // PFSPScheduler.LOG.debug("new task: '" + tid
      // + "' length: " + length);
    }

    return new JobDurationInfo(job.getJobID(), tasks, TaskType.MAP);

  }

  @Deprecated
  public static final JobDurationInfo fromJobInProgressReducePhase(
      final JobInProgress job) {
    Map<TaskID, TaskDurationInfo> tasks = new TreeMap<TaskID, TaskDurationInfo>();

    // for reduce jobs
    long reduceDuration = 0;

    // for (Group group : job.getJobCounters()) {
    // PFSPScheduler.LOG.debug("Counter group: " + group.getName());
    // for (org.apache.hadoop.mapred.Counters.Counter counter : group) {
    // PFSPScheduler.LOG.debug("\t" + counter.getName()
    // + ": " + counter.getCounter());
    // }
    // }
    //
    long outputMap = 0;
    // job.getJobCounters().getCounter(Counter.MAP_OUTPUT_BYTES);

    // Calculate map phase output
    for (TaskInProgress task : job.getTasks(TaskType.MAP)) {
      outputMap += task.getCounters().getCounter(Counter.MAP_OUTPUT_BYTES);
    }

    LOG.debug("create duration for job: " + job.getJobID() + " type: REDUCE"
        + " len(outputMap): " + outputMap + " tasks(" + job.numReduceTasks
        + "):");

    if (job.getStatus().mapProgress() == 0.0f || job.numReduceTasks <= 0) {
      reduceDuration = 0;
    } else {
      reduceDuration = (long) Math.ceil((outputMap / Double.valueOf(job
          .getStatus().mapProgress())) / job.numReduceTasks);
    }

    long duration;
    for (TaskInProgress task : job.getTasks(TaskType.REDUCE)) {
      duration = reduceDuration;
      tasks.put(task.getTIPId(),
          new TaskDurationInfo(task.getTIPId(), duration));
    }

    return new JobDurationInfo(job.getJobID(), tasks, TaskType.REDUCE);
  }

  public final long getPhaseDuration() {
    long duration = 0;
    Map<TaskID, TaskDurationInfo> tasks = getTasks();
    for (TaskDurationInfo task : tasks.values()) {
      duration += task.getDuration();
    }
    assert duration >= 0;
    return duration;
  }

  public final long getPhaseTotalDuration() {
    long duration = 0;
    Map<TaskID, TaskDurationInfo> tasks = getTasks();
    for (TaskDurationInfo task : tasks.values()) {
      duration += task.getTotalDuration();
    }
    return duration;
  }

  public Map<TaskID, Long> getTIDSWithDurations() {
    Map<TaskID, TaskDurationInfo> tidsAndDurations = this.getTasks();
    Map<TaskID, Long> result = new HashMap<TaskID, Long>();
    for (Entry<TaskID, TaskDurationInfo> entry : tidsAndDurations.entrySet()) {
      result.put(entry.getKey(), entry.getValue().getDuration());
    }
    return result;
  }

  @Override
  public String toString() {
    return this.getPhaseDuration() + "/" + this.getPhaseTotalDuration();
  }

  public boolean hasFinishPosition() {
    return this.finishPosition > 0;
  }

  public int getFinishPosition() {
    return this.finishPosition;
  }

  public static void setFinishPosition(JobDurationInfo jdi) {
    assert (jdi.finishPosition == 0);
    if (jdi.getType() == TaskType.MAP) {
      jdi.finishPosition = currentMapFinishPos;
      currentMapFinishPos++;
    } else {
      jdi.finishPosition = currentReduceFinishPos;
      currentReduceFinishPos++;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof JobDurationInfo))
      return false;

    JobDurationInfo other = (JobDurationInfo) obj;
    return this.getJobID().equals(other.getJobID());
  }
}

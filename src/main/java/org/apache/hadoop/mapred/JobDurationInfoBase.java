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

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.mapreduce.TaskType;

public class JobDurationInfoBase<TaskDurationInfoClass extends TaskDurationInfoBase>
    implements Comparable<JobDurationInfoBase<TaskDurationInfoClass>> {

  private final JobID jobID;
  private final Map<TaskID, TaskDurationInfoClass> tasks;
  private final TaskType type;

  public JobDurationInfoBase(final JobID jobID,
      final Map<TaskID, TaskDurationInfoClass> tasks, final TaskType type) {
    this.jobID = jobID;
    this.tasks = tasks;
    this.type = type;
  }

  public final JobID getJobID() {
    return jobID;
  }

  public final Map<TaskID, TaskDurationInfoClass> getTasks() {
    return tasks;
  }

  public final boolean isFinished() {
    for (TaskDurationInfoBase task : tasks.values()) {
      if (!task.isFinished()) {
        return false;
      }
    }
    return true;
  }

  public final Map<TaskID, TaskDurationInfoClass> getWaitingTasks(
      final TaskType type) {
    Map<TaskID, TaskDurationInfoClass> result = new TreeMap<TaskID, TaskDurationInfoClass>();

    Map<TaskID, TaskDurationInfoClass> tasks = this.tasks;

    for (Entry<TaskID, TaskDurationInfoClass> entry : tasks.entrySet()) {
      if (!(entry.getValue().isFinished() || entry.getValue().isRunning())) {
        result.put(entry.getKey(), entry.getValue());
      }
    }

    return result;
  }

  public TaskType getType() {
    return type;
  }

  // TODO: check if the use of the job ID for equals and hashCode
  // can lead to problems:

  /**
   * Two duration infos are equal iff they are from the same job
   */
  @Override
  public boolean equals(final Object obj) {
    try {
      @SuppressWarnings("unchecked")
      JobDurationInfoBase<TaskDurationInfoClass> casted = (JobDurationInfoBase<TaskDurationInfoClass>) obj;
      return casted.getJobID().equals(this.getJobID());
    } catch (ClassCastException e) {
      return false;
    }
  }

  @Override
  public int compareTo(JobDurationInfoBase<TaskDurationInfoClass> o) {
    return this.jobID.compareTo(o.jobID);
  }

  @Override
  public int hashCode() {
    return this.getJobID().hashCode();
  }
}

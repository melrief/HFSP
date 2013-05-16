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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskType;

import util.CircularIterator;

public class MaxMinFSScheduler<JobDurationInfoClass extends JobDurationInfoBase<TaskDurationInfoClass>, TaskDurationInfoClass extends TaskDurationInfoBase>
    implements IVirtualScheduler<JobDurationInfoClass, TaskDurationInfoClass> {

  private static final Log LOG = LogFactory.getLog(MaxMinFSScheduler.class);

  /**
   * Assign tasks of jobs to cluster's slots using a maxmin fair share. It
   * doesn't override slots with assigned tasks.
   * 
   */
  @Override
  public final VirtualCluster<TaskDurationInfoClass> schedule(
      final TaskType type, final Collection<JobDurationInfoClass> jobs,
      final VirtualCluster<TaskDurationInfoClass> cluster) {

    if (jobs == null || jobs.isEmpty()) {
      LOG.debug("MaxMinFS: called schedule with empty job list");
      return cluster;
    }

    CircularIterator<JobDurationInfoClass> iter = new CircularIterator<JobDurationInfoClass>(
        jobs);
    List<TaskDurationInfoClass> slots;
    if (type == TaskType.MAP) {
      slots = cluster.getMapSlots();
    } else {
      slots = cluster.getReduceSlots();
    }

    int remainingJobs;
    JobDurationInfoBase<TaskDurationInfoClass> jobInfo;

    Map<TaskID, Integer> assignedTasks = new TreeMap<TaskID, Integer>();

    for (int i = 0; i < slots.size(); i++) {
      if (slots.get(i) != null) {
        // continue;
        cluster.freeSlot(type, i);
      }
      remainingJobs = jobs.size();
      jobInfo = iter.next();
      Map<TaskID, TaskDurationInfoClass> waitingTasks = jobInfo
          .getWaitingTasks(type);

      while (waitingTasks.isEmpty() && remainingJobs > 0) {
        // LOG.debug("\tMaxMinFS: Job '" + jobInfo.getJobID() + "' has "
        // + waitingTasks.size() + " waiting tasks");
        jobInfo = iter.next();
        waitingTasks = jobInfo.getWaitingTasks(type);
        remainingJobs -= 1;
      }

      // If I did a cycle exit the method, there are no tasks left
      if (remainingJobs <= 0) {
        // System.out.println("\tno tasks for slot " + i);
        break;
      }

      TaskDurationInfoClass taskAssigned = waitingTasks.values().iterator()
          .next();
      cluster.assignSlot(type, i, taskAssigned);
      assignedTasks.put(taskAssigned.getTaskID(), i);
    }

    if (LOG.isDebugEnabled()) {
      StringBuilder builder = new StringBuilder("assignedTasks(");
      builder.append(type).append("): assignedTasks: ").append(assignedTasks)
          .append(" => virtual cluster: [");
      boolean first = true;
      for (TaskDurationInfoClass tdi : slots) {
        if (first)
          first = false;
        else
          builder.append(", ");
        builder.append(tdi == null ? "none" : tdi.getTaskID());
      }
      builder.append("]");
      LOG.debug(builder.toString());
    }

    return cluster;
  }

}

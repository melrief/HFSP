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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.mapreduce.TaskType;

/**
 * Simulates the progress of jobs in the virtual cluster with the specific
 * scheduler.
 * 
 * The expander class must provide
 * {@link VirtualProgressManager#update(Set, Object)} method that upgrade jobs
 * virtual time.
 * 
 * @author Mario Pastorelli
 */
public abstract class VirtualProgressManager<JobDurationInfoClass extends JobDurationInfoBase<TaskDurationInfoClass>, TaskDurationInfoClass extends TaskDurationInfoBase, IntervalInfoClass> {

  private VirtualCluster<TaskDurationInfoClass> cluster;
  private IVirtualScheduler<JobDurationInfoClass, TaskDurationInfoClass> scheduler;
  private Set<JobDurationInfoClass> mapJobDurationInfos;
  private Set<JobDurationInfoClass> reduceJobDurationInfos;

  public VirtualProgressManager(
      final VirtualCluster<TaskDurationInfoClass> virtualCluster,
      final IVirtualScheduler<JobDurationInfoClass, TaskDurationInfoClass> virtualScheduler) {
    this.cluster = virtualCluster;
    this.scheduler = virtualScheduler;
    this.mapJobDurationInfos = new TreeSet<JobDurationInfoClass>();
    this.reduceJobDurationInfos = new TreeSet<JobDurationInfoClass>();
  }

  public final VirtualCluster<TaskDurationInfoClass> getCluster() {
    return cluster;
  }

  public final IVirtualScheduler<JobDurationInfoClass, TaskDurationInfoClass> getScheduler() {
    return scheduler;
  }

  public abstract void update(final IntervalInfoClass interval);

  public final boolean addJobDurationInfo(final JobDurationInfoClass info) {
    // System.out.println("Adding job " + info.getJobID() + " with "
    // + info.getTasks(TaskType.MAP).size() + " map tasks");

    TaskType type = info.getType();
    boolean res = (type == TaskType.MAP) ? this.mapJobDurationInfos.add(info)
        : this.reduceJobDurationInfos.add(info);
    if (!res)
      return res; // nothing changed

    this.schedule(type);

    return res;
  }

  public final boolean updateJobDurationInfo(final JobDurationInfoClass info) {
    // System.out.println("Adding job " + info.getJobID() + " with "
    // + info.getTasks(TaskType.MAP).size() + " map tasks");

    TaskType type = info.getType();

    Set<JobDurationInfoClass> jdis = (type == TaskType.MAP) ? this.mapJobDurationInfos
        : this.reduceJobDurationInfos;

    if (!jdis.contains(info))
      return false;

    jdis.remove(info);
    boolean res = jdis.add(info);

    if (!res)
      return res;

    List<TaskDurationInfoClass> slots = this.cluster.getSlots(type);
    for (int i = 0; i < slots.size(); i++) {
      TaskDurationInfoClass slot = slots.get(i);
      if (slot != null && slot.getTaskID().getJobID().equals(info.getJobID())) {
        this.cluster.freeSlot(type, i);
      }
    }

    this.schedule(type);

    return res;
  }

  public boolean setSlotsNum(final int num, final TaskType type) {
    boolean success = this.cluster.setSlotsNum(num, type);
    if (success) {
      this.schedule(type);
    }
    return success;
  }

  public Set<JobDurationInfoClass> getMapDurationInfos() {
    return this.mapJobDurationInfos;
  }

  public Set<JobDurationInfoClass> getReduceDurationInfos() {
    return this.reduceJobDurationInfos;
  }

  public Set<JobDurationInfoClass> getDurationInfos(TaskType type) {
    return type == TaskType.MAP ? this.getMapDurationInfos() : this
        .getReduceDurationInfos();
  }

  // TODO make it more efficient
  public JobDurationInfoClass getDurationInfo(JobID jobID, TaskType type) {
    for (JobDurationInfoClass jdi : this.getDurationInfos(type)) {
      if (jdi.getJobID().equals(jobID))
        return jdi;
    }
    return null;
  }

  private void schedule(TaskType type) {
    this.scheduler.schedule(type, this.getDurationInfos(type), this.cluster);
  }
}

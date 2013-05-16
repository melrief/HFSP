package org.apache.hadoop.mapred;

import org.apache.hadoop.mapreduce.TaskType;

public class NoPreemption implements PreemptionStrategy {

  @Override
  public boolean isPreemptionActive() {
    return false;
  }

  @Override
  public boolean canBePreempted(TaskStatus status) {
    return false;
  }

  @Override
  public boolean canBeResumed(TaskStatus status) {
    return false;
  }

  @Override
  public boolean preempt(TaskInProgress tip, TaskStatus status) {
    return false;
  }

  @Override
  public boolean resume(TaskInProgress tip, TaskStatus status) {
    return false;
  }

  @Override
  public boolean isPreempted(TaskStatus status) {
    return false;
  }

  @Override
  public int getPreemptedTasks(JobInProgress jip, TaskType type) {
    return 0;
  }

  @Override
  public String toString() {
    return "None";
  }
}

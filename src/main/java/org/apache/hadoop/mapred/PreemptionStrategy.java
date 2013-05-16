package org.apache.hadoop.mapred;

import org.apache.hadoop.mapreduce.TaskType;

public interface PreemptionStrategy {

  public boolean isPreemptionActive();
  
  public boolean canBePreempted(TaskStatus status);
  
  public boolean canBeResumed(TaskStatus status);
  
  public boolean preempt(TaskInProgress tip, TaskStatus status);
  
  public boolean resume(TaskInProgress tip, TaskStatus status);
  
  public boolean isPreempted(TaskStatus status);
  
  public int getPreemptedTasks(JobInProgress jip, TaskType type);
}

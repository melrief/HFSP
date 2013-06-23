package org.apache.hadoop.mapred;

public class ClaimedSlots {
  private final int numPreemptedForNewTasks;
  private final int numPreemptedForResumableTasks;
  
  public ClaimedSlots(final int numPreemptedForNewTasks
                    , final int numPreemptedForResumableTasks) {
    this.numPreemptedForNewTasks = numPreemptedForNewTasks;
    this.numPreemptedForResumableTasks = numPreemptedForResumableTasks;
  }

  public int getNumPreemptedForNewTasks() {
    return this.numPreemptedForNewTasks;
  }

  public int getNumPreemptedForResumableTasks() {
    return this.numPreemptedForResumableTasks;
  }
  
  
}

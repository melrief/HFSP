package org.apache.hadoop.mapred;

import java.util.TreeMap;

import org.apache.hadoop.mapred.UtilsForTests.FakeClock;
import org.apache.hadoop.mapreduce.split.JobSplit;

public class HFSPFakeTaskInProgress extends TaskInProgress {
  private Clock clock;
  private boolean isMap;
  private HFSPFakeJobInProgress fakeJob;
  private TreeMap<TaskAttemptID, String> activeTasks;
  private TaskStatus taskStatus;
  private boolean isComplete = false;
  String[] inputLocations;

  // Constructor for map
  HFSPFakeTaskInProgress(JobID jId, JobTracker jobTracker, boolean isMap,
      int id, JobConf jobConf, HFSPFakeJobInProgress job,
      String[] inputLocations, JobSplit.TaskSplitMetaInfo split, FakeClock clock) {
    super(jId, "", split, jobTracker, jobConf, job, id, 1);
    this.clock = clock;
    this.isMap = isMap;
    this.fakeJob = job;
    this.inputLocations = inputLocations;
    activeTasks = new TreeMap<TaskAttemptID, String>();
    taskStatus = TaskStatus.createTaskStatus(isMap);
    taskStatus.setRunState(TaskStatus.State.UNASSIGNED);
  }

  @Override
  public boolean isMapTask() {
    return this.isMap;
  }

  void createTaskAttempt(Task task, String taskTracker) {
    activeTasks.put(task.getTaskID(), taskTracker);
    taskStatus = TaskStatus.createTaskStatus(isMap, task.getTaskID(), 0.5f, 1,
        TaskStatus.State.RUNNING, "", "", "", TaskStatus.Phase.STARTING,
        new Counters());
    taskStatus.setStartTime(this.clock.getTime());
  }

  @Override
  TreeMap<TaskAttemptID, String> getActiveTasks() {
    return activeTasks;
  }

  public synchronized boolean isComplete() {
    return isComplete;
  }

  public boolean isRunning() {
    return activeTasks.size() > 0;
  }

  @Override
  public TaskStatus getTaskStatus(TaskAttemptID taskid) {
    return taskStatus;
  }

  void killAttempt() {
    if (isMap) {
      fakeJob.mapTaskFinished(this);
    } else {
      fakeJob.reduceTaskFinished(this);
    }
    activeTasks.clear();
    taskStatus.setRunState(TaskStatus.State.UNASSIGNED);
  }

  void finishAttempt(long startTime, long finishTime) {
    this.isComplete = true;
    if (isMap) {
      this.fakeJob.mapTaskFinished(this);
    } else {
      this.fakeJob.reduceTaskFinished(this);
    }
    this.activeTasks.clear();
    this.taskStatus.setRunState(TaskStatus.State.SUCCEEDED);
    this.taskStatus.setStartTime(startTime);
    this.taskStatus.setFinishTime(finishTime);
  }

  @Override
  public TaskAttemptID getSuccessfulTaskid() {
    return this.taskStatus.getTaskID();
  }
}

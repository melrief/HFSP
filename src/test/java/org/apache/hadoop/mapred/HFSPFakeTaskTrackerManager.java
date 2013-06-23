package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

public class HFSPFakeTaskTrackerManager implements TaskTrackerManager {
  int maps = 0;
  int reduces = 0;
  int maxMapTasksPerTracker = 2;
  int maxReduceTasksPerTracker = 2;
  long ttExpiryInterval = 10 * 60 * 1000L; // default interval
  List<JobInProgressListener> listeners = new ArrayList<JobInProgressListener>();
  Map<JobID, JobInProgress> jobs = new HashMap<JobID, JobInProgress>();

  private Map<String, TaskTracker> trackers = new HashMap<String, TaskTracker>();
  private Map<String, TaskStatus> statuses = new HashMap<String, TaskStatus>();
  private Map<String, HFSPFakeTaskInProgress> tips = new HashMap<String, HFSPFakeTaskInProgress>();
  private Map<String, TaskTrackerStatus> trackerForTip = new HashMap<String, TaskTrackerStatus>();

  public HFSPFakeTaskTrackerManager(int numRacks, int numTrackersPerRack) {
    int nextTrackerId = 1;
    for (int rack = 1; rack <= numRacks; rack++) {
      for (int node = 1; node <= numTrackersPerRack; node++) {
        int id = nextTrackerId++;
        String host = "rack" + rack + ".node" + node;
        System.out.println("Creating TaskTracker tt" + id + " on " + host);
        TaskTracker tt = new TaskTracker("tt" + id);
        tt.setStatus(new TaskTrackerStatus("tt" + id, host, 0,
            new ArrayList<TaskStatus>(), 0, 0, maxMapTasksPerTracker,
            maxReduceTasksPerTracker));
        trackers.put("tt" + id, tt);
      }
    }
  }

  @Override
  public ClusterStatus getClusterStatus() {
    int numTrackers = trackers.size();

    return new ClusterStatus(numTrackers, 0, 0, ttExpiryInterval, maps,
        reduces, numTrackers * maxMapTasksPerTracker, numTrackers
            * maxReduceTasksPerTracker, JobTracker.State.RUNNING);
  }

  @Override
  public QueueManager getQueueManager() {
    return null;
  }

  @Override
  public int getNumberOfUniqueHosts() {
    return trackers.size();
  }

  @Override
  public Collection<TaskTrackerStatus> taskTrackers() {
    List<TaskTrackerStatus> statuses = new ArrayList<TaskTrackerStatus>();
    for (TaskTracker tt : trackers.values()) {
      statuses.add(tt.getStatus());
    }
    return statuses;
  }

  @Override
  public void addJobInProgressListener(JobInProgressListener listener) {
    listeners.add(listener);
  }

  @Override
  public void removeJobInProgressListener(JobInProgressListener listener) {
    listeners.remove(listener);
  }

  @Override
  public int getNextHeartbeatInterval() {
    return MRConstants.HEARTBEAT_INTERVAL_MIN;
  }

  @Override
  public void killJob(JobID jobid) {
    return;
  }

  @Override
  public JobInProgress getJob(JobID jobid) {
    return jobs.get(jobid);
  }

  public void initJob(JobInProgress job) {
    // do nothing
  }

  public void failJob(JobInProgress job) {
    // do nothing
  }

  // Test methods

  public void submitJob(JobInProgress job) throws IOException {
    jobs.put(job.getJobID(), job);
    for (JobInProgressListener listener : listeners) {
      listener.jobAdded(job);
    }
  }

  public TaskTracker getTaskTracker(String trackerID) {
    return trackers.get(trackerID);
  }

  public void startTask(String trackerName, Task t, HFSPFakeTaskInProgress tip) {
    final boolean isMap = t.isMapTask();
    if (isMap) {
      maps++;
    } else {
      reduces++;
    }
    String attemptId = t.getTaskID().toString();
    TaskStatus status = tip.getTaskStatus(t.getTaskID());
    TaskTrackerStatus trackerStatus = trackers.get(trackerName).getStatus();
    tips.put(attemptId, tip);
    statuses.put(attemptId, status);
    trackerForTip.put(attemptId, trackerStatus);
    status.setRunState(TaskStatus.State.RUNNING);
    trackerStatus.getTaskReports().add(status);
  }

  public void finishTask(String taskTrackerName, String attemptId, long start,
      long end) {
    HFSPFakeTaskInProgress tip = tips.get(attemptId);
    if (tip.isMapTask()) {
      maps--;
    } else {
      reduces--;
    }
    tip.finishAttempt(start, end);
    TaskStatus status = statuses.get(attemptId);
    trackers.get(taskTrackerName).getStatus().getTaskReports().remove(status);
  }

  public void finishTask(String taskTrackerName, TaskAttemptID attemptId,
      long start, long end) {
    this.finishTask(taskTrackerName, attemptId.toString(), start, end);
  }

  @Override
  public boolean killTask(TaskAttemptID attemptId, boolean shouldFail) {
    String attemptIdStr = attemptId.toString();
    HFSPFakeTaskInProgress tip = tips.get(attemptIdStr);
    if (tip.isMapTask()) {
      maps--;
    } else {
      reduces--;
    }
    tip.killAttempt();
    TaskStatus status = statuses.get(attemptIdStr);
    trackerForTip.get(attemptIdStr).getTaskReports().remove(status);
    return true;
  }

  @Override
  public boolean isInSafeMode() {
    // TODO Auto-generated method stub
    return false;
  }
}

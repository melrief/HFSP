package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.mapred.UtilsForTests.FakeClock;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.net.Node;

//import org.apache.hadoop.mapred.FakeObjectUtilities.FakeJobHistory;

public class HFSPFakeJobInProgress extends JobInProgress {

  static int jobCounter = 0;

  private HFSPFakeTaskTrackerManager taskTrackerManager;
  private int mapCounter = 0;
  private int reduceCounter = 0;
  private final String[][] mapInputLocations; // Array of hosts for each map

  private FakeClock clock;

  private JobTracker jobTracker;

  public HFSPFakeJobInProgress(JobConf jobConf, FakeClock clock,
      HFSPFakeTaskTrackerManager taskTrackerManager, JobTracker jobTracker,
      String[][] mapInputLocations, JobTracker jt) throws IOException {
    super(new JobID("test", ++(HFSPFakeJobInProgress.jobCounter)), jobConf, jt);
    this.taskTrackerManager = taskTrackerManager;
    this.jobTracker = jobTracker;
    this.clock = clock;
    this.startTime = clock.time;
    this.mapInputLocations = mapInputLocations;
    this.startTime = System.currentTimeMillis();
    this.status = new JobStatus();
    this.status.setRunState(JobStatus.PREP);
    // this.nonLocalMaps = new LinkedList<TaskInProgress>();
    this.nonLocalRunningMaps = new LinkedHashSet<TaskInProgress>();
    this.runningMapCache = new IdentityHashMap<Node, Set<TaskInProgress>>();
    this.nonRunningReduces = new HashSet<TaskInProgress>();
    this.runningReduces = new LinkedHashSet<TaskInProgress>();
    // this.jobHistory = new FakeJobHistory();
    initTasks();
  }

  @Override
  public synchronized void initTasks() throws IOException {
    // initTasks is needed to create non-empty cleanup and setup TIP
    // arrays, otherwise calls such as job.getTaskInProgress will fail
    JobID jobId = getJobID();
    JobConf conf = getJobConf();
    String jobFile = "";
    // create two cleanup tips, one map and one reduce.
    cleanup = new TaskInProgress[2];
    // cleanup map tip.
    cleanup[0] = new TaskInProgress(jobId, jobFile, null, jobtracker, conf,
        this, numMapTasks, 1);
    cleanup[0].setJobCleanupTask();
    // cleanup reduce tip.
    cleanup[1] = new TaskInProgress(jobId, jobFile, numMapTasks,
        numReduceTasks, jobtracker, conf, this, 1);
    cleanup[1].setJobCleanupTask();
    // create two setup tips, one map and one reduce.
    setup = new TaskInProgress[2];
    // setup map tip.
    setup[0] = new TaskInProgress(jobId, jobFile, null, jobtracker, conf, this,
        numMapTasks + 1, 1);
    setup[0].setJobSetupTask();
    // setup reduce tip.
    setup[1] = new TaskInProgress(jobId, jobFile, numMapTasks,
        numReduceTasks + 1, jobtracker, conf, this, 1);
    setup[1].setJobSetupTask();
    // create maps
    numMapTasks = conf.getNumMapTasks();
    maps = new TaskInProgress[numMapTasks];
    // empty format
    JobSplit.TaskSplitMetaInfo split = JobSplit.EMPTY_TASK_SPLIT;
    for (int i = 0; i < numMapTasks; i++) {
      String[] inputLocations = null;
      if (mapInputLocations != null)
        inputLocations = mapInputLocations[i];
      maps[i] = new HFSPFakeTaskInProgress(this.getJobID(), this.jobTracker,
          true, i, this.getJobConf(), this, inputLocations, split, this.clock);
      if (mapInputLocations == null) // Job has no locality info
        nonLocalMaps.add(maps[i]);
    }
    // create reduces
    numReduceTasks = conf.getNumReduceTasks();
    reduces = new TaskInProgress[numReduceTasks];
    for (int i = 0; i < numReduceTasks; i++) {
      reduces[i] = new HFSPFakeTaskInProgress(getJobID(), this.jobTracker,
          false, i, getJobConf(), this, null, split, clock);
    }
    this.tasksInited = true;
  }

  @Override
  public Task obtainNewMapTask(final TaskTrackerStatus tts, int clusterSize,
      int numUniqueHosts) throws IOException {
    return this.obtainNewMapTask(tts, clusterSize, numUniqueHosts,
        LocalityLevel.ANY.ordinal());
  }

  @Override
  public synchronized Task obtainNewNodeLocalMapTask(TaskTrackerStatus tts,
      int clusterSize, int numUniqueHosts) throws IOException {
    return this.obtainNewMapTask(tts, clusterSize, numUniqueHosts,
        LocalityLevel.NODE.ordinal());
  }

  @Override
  public synchronized Task obtainNewNodeOrRackLocalMapTask(
      TaskTrackerStatus tts, int clusterSize, int numUniqueHosts)
      throws IOException {
    return this.obtainNewMapTask(tts, clusterSize, numUniqueHosts,
        LocalityLevel.RACK.ordinal());
  }

  @Override
  public synchronized Task obtainNewNonLocalMapTask(TaskTrackerStatus tts,
      int clusterSize, int numUniqueHosts) throws IOException {
    return this.obtainNewMapTask(tts, clusterSize, numUniqueHosts,
        LocalityLevel.ANY.ordinal());
  }

  public Task obtainNewMapTask(final TaskTrackerStatus tts, int clusterSize,
      int numUniqueHosts, int localityLevel) throws IOException {
    for (int map = 0; map < maps.length; map++) {
      HFSPFakeTaskInProgress tip = (HFSPFakeTaskInProgress) maps[map];
      if (!tip.isRunning() && !tip.isComplete()
          && getLocalityLevel(tip, tts) < localityLevel) {
        TaskAttemptID attemptId = getTaskAttemptID(tip);
        JobSplit.TaskSplitMetaInfo split = JobSplit.EMPTY_TASK_SPLIT;
        Task task = new MapTask("", attemptId, 0, split.getSplitIndex(), 1) {
          @Override
          public String toString() {
            return String.format("%s on %s", getTaskID(), tts.getTrackerName());
          }
        };
        runningMapTasks++;
        tip.createTaskAttempt(task, tts.getTrackerName());
        nonLocalRunningMaps.add(tip);
        taskTrackerManager.startTask(tts.getTrackerName(), task, tip);
        return task;
      }
    }
    return null;
  }

  @Override
  public Task obtainNewReduceTask(final TaskTrackerStatus tts, int clusterSize,
      int ignored) throws IOException {
    for (int reduce = 0; reduce < reduces.length; reduce++) {
      HFSPFakeTaskInProgress tip = (HFSPFakeTaskInProgress) reduces[reduce];
      if (!tip.isRunning() && !tip.isComplete()) {
        TaskAttemptID attemptId = getTaskAttemptID(tip);
        Task task = new ReduceTask("", attemptId, 0, maps.length, 1) {
          @Override
          public String toString() {
            return String.format("%s on %s", getTaskID(), tts.getTrackerName());
          }
        };
        runningReduceTasks++;
        tip.createTaskAttempt(task, tts.getTrackerName());
        runningReduces.add(tip);
        taskTrackerManager.startTask(tts.getTrackerName(), task, tip);
        return task;
      }
    }
    return null;
  }

  public void mapTaskFinished(TaskInProgress tip) {
    runningMapTasks--;
    finishedMapTasks++;
    nonLocalRunningMaps.remove(tip);
  }

  public void reduceTaskFinished(TaskInProgress tip) {
    runningReduceTasks--;
    finishedReduceTasks++;
    runningReduces.remove(tip);
  }

  private TaskAttemptID getTaskAttemptID(TaskInProgress tip) {
    JobID jobId = getJobID();
    // TaskType type = tip.isMapTask() ? TaskType.MAP : TaskType.REDUCE;
    return new TaskAttemptID(jobId.getJtIdentifier(), jobId.getId(),
        tip.isMapTask(), tip.getIdWithinJob(), tip.nextTaskId++);
  }

  public static String getRack(String hostname) {
    // Host names are of the form rackN.nodeM, so split at the dot.
    return hostname.split("\\.")[0];
  }

  @Override
  int getLocalityLevel(TaskInProgress tip, TaskTrackerStatus tts) {
    HFSPFakeTaskInProgress ftip = (HFSPFakeTaskInProgress) tip;
    if (ftip.inputLocations != null) {
      // Check whether we're on the same host as an input split
      for (String location : ftip.inputLocations) {
        if (location.equals(tts.host)) {
          return 0;
        }
      }
      // Check whether we're on the same rack as an input split
      for (String location : ftip.inputLocations) {
        if (getRack(location).equals(getRack(tts.host))) {
          return 1;
        }
      }
      // Not on same rack or host
      return 2;
    } else {
      // Job has no locality info
      return -1;
    }
  }
}
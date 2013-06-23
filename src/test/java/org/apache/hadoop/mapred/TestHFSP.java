package org.apache.hadoop.mapred;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.mapred.HFSPScheduler.QueueType;
import org.apache.hadoop.mapred.UtilsForTests.FakeClock;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

public class TestHFSP extends TestCase {

  private JobConf conf;
  private HFSPFakeTaskTrackerManager taskTrackerManager;
  private FakeClock clock;
  private HFSPScheduler scheduler;
  private JobTracker jobTracker;
  private static int jtPort = 8012;

  @Override
  protected void setUp() throws Exception {
    HFSPFakeJobInProgress.jobCounter = 0;
    System.setProperty("hadoop.log.dir",
        "/tmp/testHFSP_" + Long.toString(System.currentTimeMillis()));
    this.setUpLog4J();
    this.setUpCluster(1, 2, false);
  }

  private void setUpLog4J() {
    // BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.INFO);
    Logger.getLogger(JobTracker.class).setLevel(Level.DEBUG);
    Logger.getLogger(HFSPScheduler.class).setLevel(Level.DEBUG);
  }

  private void setUpCluster(int numRacks, int numNodesPerRack,
      boolean assignMultiple) throws IOException, IllegalArgumentException,
      InstantiationException, IllegalAccessException,
      InvocationTargetException, InterruptedException {
    this.conf = new JobConf();
    this.conf.setInt(HFSPScheduler.TRAINER_MIN_MAPS_KEYNAME, 0);
    this.conf.setInt(CompletedTasksTrainer.NUM_MAP_COMPLETED_KEY, 1);
    this.conf.setInt(HFSPScheduler.TRAINER_MIN_REDUCES_KEYNAME, 0);
    this.conf.setInt(SojournTrainer.getSojournConfKeyname(
        HFSPScheduler.PREFIX_KEYNAME, TaskType.REDUCE), 1);

    this.conf.set("mapred.job.tracker", "localhost:0");
    this.conf.set("mapred.job.tracker.http.address", "0.0.0.0:0");

    this.taskTrackerManager = new HFSPFakeTaskTrackerManager(numRacks,
        numNodesPerRack);
    this.clock = new FakeClock();
    try {
      this.jobTracker = new JobTracker(this.conf, this.clock);
    } catch (Exception e) {
      throw new RuntimeException("Could not start JT", e);
    }
    this.scheduler = new HFSPScheduler(clock, true);
    this.scheduler.setConf(conf);
    this.scheduler.setTaskTrackerManager(taskTrackerManager);
    this.scheduler.start();
    this.advanceTime(100);
  }

  @Override
  protected void tearDown() throws Exception {
    if (this.scheduler != null) {
      this.scheduler.terminate();
    }
  }

  private JobInProgress submitJob(int state, int maps, int reduces)
      throws IOException {
    return this.submitJob(state, maps, reduces, null);
  }

  private JobInProgress submitJob(int state, int maps, int reduces,
      String[][] mapInputLocations) throws IOException {
    JobConf jobConf = new JobConf(conf);
    jobConf.setNumMapTasks(maps);
    jobConf.setNumReduceTasks(reduces);
    JobInProgress job = new HFSPFakeJobInProgress(jobConf, this.clock,
        this.taskTrackerManager, this.jobTracker, mapInputLocations,
        UtilsForTests.getJobTracker());
    job.getStatus().setRunState(state);
    taskTrackerManager.submitJob(job);
    return job;
  }

  protected void submitJobs(int number, int state, int maps, int reduces)
      throws IOException {
    for (int i = 0; i < number; i++) {
      submitJob(state, maps, reduces);
    }
  }

  private void advanceTime(long time) throws IOException {
    clock.advance(time);
    scheduler.update();
  }

  protected TaskTracker tracker(String taskTrackerName) {
    return taskTrackerManager.getTaskTracker(taskTrackerName);
  }

  /**
   * Check state after job submission and correct assignment of tasks on empty
   * slots
   */
  @Test
  public void testSubmitJobAndAssignTask() throws IOException {
    this.advanceTime(1000);

    JobInProgress submittedJIP = this.submitJob(JobStatus.RUNNING, 1, 1);

    assertTrue(this.scheduler.getJobs(null).iterator().next()
        .equals(submittedJIP));

    List<Task> tasks = this.scheduler.assignTasks(tracker("tt1"));
    assertTrue(tasks.size() == 2);
  }

  /**
   * Complete 1 task of 2 and check that the job is marked as ready and removed
   * from training
   */
  @Test
  public void testTrainToSizeBased() throws IOException {
    this.advanceTime(1000);

    JobInProgress jip = this.submitJob(JobStatus.RUNNING, 2, 2);

    assertTrue(!this.scheduler.isJobReadyForSizeBased(jip, TaskType.MAP));
    assertTrue(this.scheduler.getJob(jip.getJobID(), QueueType.TRAIN,
        TaskType.MAP).equals(jip));
    assertTrue(this.scheduler.getJob(jip.getJobID(), QueueType.SIZE_BASED,
        TaskType.MAP).equals(jip));

    List<Task> tasks = this.scheduler.assignTasks(tracker("tt1"));
    this.taskTrackerManager.finishTask("tt1", tasks.get(0).getTaskID(), 1000,
        2000);

    this.scheduler.update();

    assertTrue(this.scheduler.isJobReadyForSizeBased(jip, TaskType.MAP));
    assertTrue(this.scheduler.getJob(jip.getJobID(), QueueType.TRAIN,
        TaskType.MAP) == null);
    assertTrue(this.scheduler.getJob(jip.getJobID(), QueueType.SIZE_BASED,
        TaskType.MAP).equals(jip));
  }

  /**
   * Submit two jobs of different sizes and check the order of the job queue
   */
  @Test
  public void testInitialPriority() throws IOException {
    this.advanceTime(1l);
    JobInProgress jip1 = this.submitJob(JobStatus.RUNNING, 20, 2);
    this.advanceTime(1l);
    JobInProgress jip2 = this.submitJob(JobStatus.RUNNING, 2, 20);

    // Map priority
    Iterator<JobInProgress> jobsIter = this.scheduler.getJobs(
        QueueType.SIZE_BASED, TaskType.MAP).iterator();
    assertTrue(jobsIter.next().equals(jip2));
    assertTrue(jobsIter.next().equals(jip1));
    JobDurationInfo duration1 = this.scheduler.getDuration(jip1.getJobID(),
        TaskType.MAP);
    JobDurationInfo duration2 = this.scheduler.getDuration(jip2.getJobID(),
        TaskType.MAP);
    assertTrue(duration1.getPhaseTotalDuration() == 10 * duration2
        .getPhaseTotalDuration());
    assertTrue(HFSPScheduler.JOB_DURATION_COMPARATOR.compare(duration1,
        duration2) > 0);

    // Reduce priority
    Iterator<JobInProgress> jobsReduceIter = this.scheduler.getJobs(
        QueueType.SIZE_BASED, TaskType.REDUCE).iterator();
    assertTrue(jobsReduceIter.next().equals(jip1));
    assertTrue(jobsReduceIter.next().equals(jip2));
    JobDurationInfo reduceDuration1 = this.scheduler.getDuration(
        jip1.getJobID(), TaskType.REDUCE);
    JobDurationInfo reduceDuration2 = this.scheduler.getDuration(
        jip2.getJobID(), TaskType.REDUCE);
    System.out.println(reduceDuration1 + " " + reduceDuration2);
    assertTrue(reduceDuration1.getPhaseTotalDuration() * 10 == reduceDuration2
        .getPhaseTotalDuration());
    assertTrue(HFSPScheduler.JOB_DURATION_COMPARATOR.compare(reduceDuration1,
        reduceDuration2) < 0);

    // Call assign tasks with 2 map and 2 reduce free slots and check the
    // assignment
    List<Task> tasks = this.scheduler.assignTasks(tracker("tt1"));

    assertTrue(tasks.get(0).getJobID().equals(jip2.getJobID()));
    assertTrue(tasks.get(1).getJobID().equals(jip2.getJobID()));
    assertTrue(tasks.get(2).getJobID().equals(jip1.getJobID()));
    assertTrue(tasks.get(3).getJobID().equals(jip1.getJobID()));
  }
}

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
/*
 * 
 */
package org.apache.hadoop.mapred;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.AcceptConfigurationManagerVisitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ConfigurationDescriptionToXMLConverter;
import org.apache.hadoop.conf.ConfigurationManager;
import org.apache.hadoop.conf.Configurator;
import org.apache.hadoop.conf.FieldType;
import org.apache.hadoop.mapred.AssignTasksHelper.HelperForType;
import org.apache.hadoop.mapred.AssignTasksHelper.Phase;
import org.apache.hadoop.mapred.AssignTasksHelper.TaskStatuses;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Double queue scheduler in which one queue is used for training and the other
 * one for size-based scheduling
 * 
 * @author Mario Pastorelli
 * 
 */
// FIXME: fix mock mode
public class HFSPScheduler extends TaskScheduler implements
    AcceptConfigurationManagerVisitor {

  static {
    Configuration.addDefaultResource("hfsp-scheduler.xml");
  }

  public static final String PREFIX_KEYNAME = "mapred.hfsp-scheduler.";
  public static final String TRAIN_MAP_SLOTS_KEYNAME = PREFIX_KEYNAME
      + "train-map-slots";
  public static final String TRAIN_REDUCE_SLOTS_KEYNAME = PREFIX_KEYNAME
      + "train-reduce-slots";
  public static final String UPDATE_INTERVAL_KEYNAME = PREFIX_KEYNAME
      + "update-interval";
  public static final String PREEMPTION_STRATEGY_CLASS_KEY = PREFIX_KEYNAME
      + "preemption-strategy";
  public static final String DELAY_KEYNAME = PREFIX_KEYNAME + "delay.enabled";
  public static final String INITIAL_MAP_DURATION = PREFIX_KEYNAME
      + "initial-map-duration";
  public static final String INITIAL_REDUCE_DURATION = PREFIX_KEYNAME
      + "initial-reduce-duration";
  public static final String DURATION_MODIFIER_MAP = PREFIX_KEYNAME
      + "duration-modifier-map";
  public static final String DURATION_MODIFIER_REDUCE = PREFIX_KEYNAME
      + "duration-modifier-reduce";

  public static final String TRAINER_KEYNAME = PREFIX_KEYNAME + "trainer.";
  public static final String TRAINER_MIN_MAPS_KEYNAME = TRAINER_KEYNAME
      + "min-maps";
  public static final String TRAINER_MIN_REDUCES_KEYNAME = TRAINER_KEYNAME
      + "min-reduces";

  // Maximum locality delay when auto-computing locality delays
  private static final long MAX_AUTOCOMPUTED_LOCALITY_DELAY = 15000;

  /* private */final static Log LOG = LogFactory.getLog(HFSPScheduler.class);

  /** Utility for iterate over maps and reduces */
  public final static TaskType[] TASK_TYPES = new TaskType[] { TaskType.MAP,
      TaskType.REDUCE };

  /** Comparator to decide which job must be trained before */
  private static final ProcessedTasksJobComparator TRAIN_COMPARATOR_MAP = new ProcessedTasksJobComparator(
      TaskType.MAP);

  /** Comparator to decide which job must be trained before */
  private static final ProcessedTasksJobComparator TRAIN_COMPARATOR_REDUCE = new ProcessedTasksJobComparator(
      TaskType.REDUCE);

  /** Fallback comparator to be used based on the number of remaining tasks */
  public static final RemainingTasksJobComparator FALLBACK_COMPARATOR_MAP = new RemainingTasksJobComparator(
      TaskType.MAP);

  /** Fallback comparator to be used based on the number of remaining tasks */
  public static final RemainingTasksJobComparator FALLBACK_COMPARATOR_REDUCE = new RemainingTasksJobComparator(
      TaskType.REDUCE);

  /** Comparator of two jobs durations */
  public static final JobDurationComparator JOB_DURATION_COMPARATOR = new JobDurationComparator();

  /** The object responsible for the configuration of the scheduler */
  protected ConfigurationManager<HFSPScheduler> configurationManager;

  /** Default task duration when no job has been trained */
  private long initialMapTaskDuration;

  /** Default task duration when no job has been trained */
  private long initialReduceTaskDuration;

  /** Initial duration modifier (<=0 optimistic, >0 pessimistic) */
  private float durationModifierMap;

  /** Initial duration modifier (<=0 optimistic, >0 pessimistic) */
  private float durationModifierReduce;

  /** Max slots assigned to map training */
  int numSlotsForMapTrain = 0;

  /** Max slots assigned to reduce training */
  int numSlotsForReduceTrain = 0;

  /** Number of tasks assigned to a job for training */
  int numTasksForMapTrain = 0;

  /** Number of tasks assigned to a job for training */
  int numTasksForReduceTrain = 0;

  /** The two type of queue */
  enum QueueType {
    TRAIN, SIZE_BASED
  };

  /** Jobs in training mode */
  Set<JobInProgress> trainingMapJobs;

  /** Jobs in training mode */
  Set<JobInProgress> trainingReduceJobs;

  /** When the last event (update) occurred */
  private long lastEvent = 0l;

  /** The virtual cluster for job progress simulation */
  private VirtualCluster<TaskDurationInfo> cluster;

  /** The scheduler used for job progress simulation */
  private IVirtualScheduler<JobDurationInfo, TaskDurationInfo> scheduler;

  /**
   * The job progress simulator
   * 
   * FIXME: should be private with some access way
   */
  VirtualProgressManager<JobDurationInfo, TaskDurationInfo, Interval> progressManager;

  /**
   * Priority queue of JobInProgress ordered by map job duration info
   * 
   * JobDurationInfo is mutable so resort is needed when the duration changes
   */
  private TreeMap<JobDurationInfo, JobInProgress> sizeBasedMapJobsQueue;

  /**
   * Priority queue of JobInProgress ordered by reduce job duration info
   * 
   * JobDurationInfo is mutable so resort is needed when the duration changes
   */
  private TreeMap<JobDurationInfo, JobInProgress> sizeBasedReduceJobsQueue;

  /** Top-level JobInProgress listener, this add new jobs to the scheduler */
  protected HFSPJIPListener jobInProgressListener;

  /** Retrieve the map JobInProgress from the JobID */
  public Map<JobID, JobInProgress> jIDToJIP;

  /** Retrieve the map JobDurationInfo from the JobID */
  public Map<JobID, JobDurationInfo> jIDToMapJDI;

  /** Retrieve the reduce JobDurationInfo from the JobID */
  public Map<JobID, JobDurationInfo> jIDToReduceJDI;

  /** Job trainer for the train queue */
  Trainer<JobDurationInfo> trainer;

  /** Thread used to train jobs without interfering with the train scheduler */
  UpdateThread updateThread;

  /**
   * Task trackers available used for easy lookup. Otherwise we should use
   * {@link TaskTrackerManager#taskTrackers()} that returns a simple collection
   * with lookup O(n)
   */
  protected Map<String, TaskTrackerStatus> taskTrackers;

  /** Num map slots for job training */
  int numMapTrainSlotsForJob;

  /** Num reduce slots for job training */
  int numReduceTrainSlotsForJob;

  /**
   * Used to do job initialization.
   * 
   * @see {@link JobQueueTaskScheduler#eagerTaskInitializationListener}
   */
  EagerTaskInitializationListener eagerTaskInitializationListener;

  /** Clock used to take time */
  protected Clock clock;

  /** For testing/debugging purpose */
  protected boolean mockMode;

  /** Interval (milliseconds) between two update */
  protected Long updateInterval;

  /** Preemption strategy */
  PreemptionStrategy preemptionStrategy;

  /**
   * Object that encapsulate utilities for
   * {@link HFSPScheduler#assignTasks(TaskTracker)}
   */
  private AssignTasksHelper taskHelper = new AssignTasksHelper(this);

  /** {@link FairScheduler#lastHeartbeatTime} */
  private long lastHeartbeatTime;

  /** {@link FairScheduler#localityDelay} */
  private long localityDelay;

  /** Lightweight version of {@link FairScheduler.JobInfo} */
  static class JobInfo {
    LocalityLevel lastMapLocalityLevel; // Locality level of last map launched
    long timeWaitedForLocalMap; // Time waiting for local map since last map
    boolean skippedAtLastHeartbeat; // Was job skipped at previous assignTasks?
                                    // (used to update timeWaitedForLocalMap)

    public JobInfo() {
      this.lastMapLocalityLevel = LocalityLevel.NODE;
    }

    @Override
    public String toString() {
      return "JobLocalityInfo(lastMapLocalityLevel: "
          + this.lastMapLocalityLevel + ", timeWaitedForLocalMap: "
          + this.timeWaitedForLocalMap + ", skippedAtLastHeartbeat: "
          + this.skippedAtLastHeartbeat + ")";
    }
  }

  /** {@link FairScheduler#infos} */
  private HashMap<JobID, JobInfo> infos = new HashMap<JobID, JobInfo>();

  /** {@link FairScheduler#autoComputeLocalityDelay} */
  private boolean autoComputeLocalityDelay;

  /** If the delay scheduling is active or not */
  private boolean delayEnabled;

  /**
   * Default constructor
   * 
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws IllegalArgumentException
   */
  public HFSPScheduler() throws IllegalArgumentException,
      InstantiationException, IllegalAccessException, InvocationTargetException {
    this(new Clock(), false);
  }

  /**
   * Constructor to be used for testing/debugging
   * 
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws IllegalArgumentException
   */
  public HFSPScheduler(Clock clock, boolean mockMode)
      throws IllegalArgumentException, InstantiationException,
      IllegalAccessException, InvocationTargetException {
    super();
    this.clock = clock;
    this.mockMode = mockMode;

    // Configure the configuration manager
    this.configurationManager = ConfigurationManager.createFor(this);

    // this.numSlotsForMapTrain = conf.getInt(TRAIN_MAP_SLOTS_KEYNAME, 0);
    this.configurationManager.addConfiguratorFor(FieldType.Integer,
        TRAIN_MAP_SLOTS_KEYNAME,
        "number of slots assigned to the map train phase", 0,
        new Configurator<Integer, HFSPScheduler>() {
          protected void set(HFSPScheduler obj, Integer value) {
            obj.numSlotsForMapTrain = value;
          }
        });

    // this.numSlotsForReduceTrain = conf.getInt(TRAIN_REDUCE_SLOTS_KEYNAME, 0);
    this.configurationManager.addConfiguratorFor(FieldType.Integer,
        TRAIN_REDUCE_SLOTS_KEYNAME,
        "number of slots assigned to the reduce train phase", 0,
        new Configurator<Integer, HFSPScheduler>() {
          protected void set(HFSPScheduler obj, Integer value) {
            obj.numSlotsForReduceTrain = value;
          }
        });

    // this.numTasksForMapTrain = conf.getInt(TRAINER_MIN_MAPS_KEYNAME, 2);
    // this.numMapTrainSlotsForJob = conf.getInt(TRAINER_MIN_MAPS_KEYNAME, 2);
    this.configurationManager.addConfiguratorFor(FieldType.Integer,
        TRAINER_MIN_MAPS_KEYNAME,
        "jobs with a number of maps smaller than this avoid the train "
            + "and their size  is set to the smallest possible", 2,
        new Configurator<Integer, HFSPScheduler>() {
          protected void set(HFSPScheduler obj, Integer value) {
            obj.numTasksForMapTrain = value;
            obj.numMapTrainSlotsForJob = value;
          }
        });

    // this.numTasksForReduceTrain = conf.getInt(TRAIN_REDUCE_SLOTS_KEYNAME, 2);
    // this.numReduceTrainSlotsForJob = conf.getInt(TRAINER_MIN_REDUCES_KEYNAME,
    // 2);
    this.configurationManager.addConfiguratorFor(FieldType.Integer,
        TRAINER_MIN_REDUCES_KEYNAME,
        "jobs with a number of reduces smaller than this avoid the train "
            + "and their size  is set to the smallest possible", 2,
        new Configurator<Integer, HFSPScheduler>() {
          protected void set(HFSPScheduler obj, Integer value) {
            obj.numTasksForReduceTrain = value;
            obj.numReduceTrainSlotsForJob = value;
          }
        });

    // this.delayEnabled = conf.getBoolean(DELAY_KEYNAME, false);
    this.configurationManager.addConfiguratorFor(FieldType.Boolean,
        DELAY_KEYNAME, "if the scheduler should use or not the delay schedule",
        false, new Configurator<Boolean, HFSPScheduler>() {
          protected void set(HFSPScheduler obj, Boolean value) {
            obj.delayEnabled = value;
          }
        });

    // this.localityDelay = conf.getLong("mapred.fairscheduler.locality.delay",
    // -1);
    this.configurationManager.addConfiguratorFor(FieldType.Long,
        "mapred.fairscheduler.locality.delay",
        "the delay for data-locality mappers (see delay scheduler)", -1l,
        new Configurator<Long, HFSPScheduler>() {
          protected void set(HFSPScheduler obj, Long value) {
            obj.localityDelay = value;
          }
        });

    // this.initialMapTaskDuration = conf.getLong(INITIAL_MAP_DURATION, 60000l);
    this.configurationManager.addConfiguratorFor(FieldType.Long,
        INITIAL_MAP_DURATION,
        "duration of a map of a not trained job when no jobs are completed",
        60000l, new Configurator<Long, HFSPScheduler>() {
          protected void set(HFSPScheduler obj, Long value) {
            obj.initialMapTaskDuration = value;
          }
        });

    // this.initialReduceTaskDuration = conf.getLong(INITIAL_REDUCE_DURATION,
    // 60000l);
    this.configurationManager.addConfiguratorFor(FieldType.Long,
        INITIAL_REDUCE_DURATION,
        "duration of a reduce of a not trained job when no jobs are completed",
        60000l, new Configurator<Long, HFSPScheduler>() {
          protected void set(HFSPScheduler obj, Long value) {
            obj.initialReduceTaskDuration = value;
          }
        });

    // this.durationModifierMap = conf.getFloat(DURATION_MODIFIER_MAP, 1.0f);
    this.configurationManager.addConfiguratorFor(FieldType.Float,
        DURATION_MODIFIER_MAP, "number multiplied to the size of each map",
        1.0f, new Configurator<Float, HFSPScheduler>() {
          protected void set(HFSPScheduler obj, Float value) {
            obj.durationModifierMap = value;
          }
        });

    // this.durationModifierReduce = conf.getFloat(DURATION_MODIFIER_REDUCE,
    // 1.0f);
    this.configurationManager.addConfiguratorFor(FieldType.Float,
        DURATION_MODIFIER_REDUCE,
        "number multiplied to the size of each reduce", 1.0f,
        new Configurator<Float, HFSPScheduler>() {
          protected void set(HFSPScheduler obj, Float value) {
            obj.durationModifierReduce = value;
          }
        });

    // this.updateInterval = conf.getLong(UPDATE_INTERVAL_KEYNAME, 5000);
    this.configurationManager.addConfiguratorFor(FieldType.Long,
        UPDATE_INTERVAL_KEYNAME,
        "after how much time the internal state of HFSP is updated", 5000l,
        new Configurator<Long, HFSPScheduler>() {
          protected void set(HFSPScheduler obj, Long value) {
            obj.updateInterval = value;
          }
        });

    // Trainer
    try {
      this.trainer = new CompositeTrainer(this, conf, this.clock);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * TODO: for now this method update just the eager task initializer, add
   * updates for all the other states
   */
  @Override
  public synchronized void setConf(final Configuration conf) {
    super.setConf(conf);
    this.trainer.setConf(conf);
    this.eagerTaskInitializationListener = new EagerTaskInitializationListener(
        conf);
    if (conf != null) {
      this.configurationManager.configure(this.getConf());
    }
  }

  @Override
  public synchronized void start() throws IOException {
    super.start();

    this.preemptionStrategy = this.loadPreemptionStrategyInstance(conf);

    // Configuration conf = getConf();
    //
    // this.numSlotsForMapTrain = conf.getInt(TRAIN_MAP_SLOTS_KEYNAME, 0);
    // this.numSlotsForReduceTrain = conf.getInt(TRAIN_REDUCE_SLOTS_KEYNAME, 0);
    // this.numMapTrainSlotsForJob = conf.getInt(TRAINER_MIN_MAPS_KEYNAME, 2);
    // this.numReduceTrainSlotsForJob = conf
    // .getInt(TRAINER_MIN_REDUCES_KEYNAME, 2);

    // this.delayEnabled = conf.getBoolean(DELAY_KEYNAME, false);
    // this.initialMapTaskDuration = conf.getLong(INITIAL_MAP_DURATION, 60000l);
    // this.initialReduceTaskDuration = conf.getLong(INITIAL_REDUCE_DURATION,
    // 60000l);
    // this.durationModifierMap = conf.getFloat(DURATION_MODIFIER_MAP, 1.0f);
    // this.durationModifierReduce = conf.getFloat(DURATION_MODIFIER_REDUCE,
    // 1.0f);
    // this.numTasksForMapTrain = conf.getInt(TRAINER_MIN_MAPS_KEYNAME, 2);
    // this.numTasksForReduceTrain = conf.getInt(TRAIN_REDUCE_SLOTS_KEYNAME, 2);

    // lookup utilities
    this.taskTrackers = new HashMap<String, TaskTrackerStatus>();
    this.jIDToJIP = new HashMap<JobID, JobInProgress>();
    this.jIDToMapJDI = Collections
        .synchronizedMap(new HashMap<JobID, JobDurationInfo>());
    this.jIDToReduceJDI = Collections
        .synchronizedMap(new HashMap<JobID, JobDurationInfo>());

    // job added listener
    this.jobInProgressListener = new HFSPJIPListener(this);
    this.taskTrackerManager.addJobInProgressListener(jobInProgressListener);

    // job initializer
    // if (!this.mockMode)
    this.eagerTaskInitializationListener
        .setTaskTrackerManager(this.taskTrackerManager);
    this.eagerTaskInitializationListener.start();
    this.taskTrackerManager
        .addJobInProgressListener(this.eagerTaskInitializationListener);
    // }

    // Training queues
    this.trainingMapJobs = Collections
        .synchronizedSet(new HashSet<JobInProgress>());
    this.trainingReduceJobs = Collections
        .synchronizedSet(new HashSet<JobInProgress>());

    // Size Based queues
    this.sizeBasedMapJobsQueue = new TreeMap<JobDurationInfo, JobInProgress>(
        HFSPScheduler.JOB_DURATION_COMPARATOR);
    this.sizeBasedReduceJobsQueue = new TreeMap<JobDurationInfo, JobInProgress>(
        HFSPScheduler.JOB_DURATION_COMPARATOR);

    // Simulator
    this.cluster = new VirtualCluster<TaskDurationInfo>(
        this.getMaxTasks(TaskType.MAP), this.getMaxTasks(TaskType.REDUCE));
    this.scheduler = new MaxMinFSScheduler<JobDurationInfo, TaskDurationInfo>();
    this.progressManager = new ProgressManager(this.cluster, this.scheduler);

    // Trainer
    // try {
    // this.trainer = new BrokerTrainer(this, conf, this.clock);
    // } catch (Exception e) {
    // throw new RuntimeException();
    // }

    // Update thread
    // this.updateInterval = conf.getLong(UPDATE_INTERVAL_KEYNAME, 5000);
    this.updateThread = new UpdateThread(this);
    // if (!this.mockMode) {
    // this.updateThread.start();
    // } else {
    // this.delayEnabled = false; // problem with delay enabled in mock mode
    // }

    // localityDelay = conf.getLong("mapred.fairscheduler.locality.delay", -1);
    if (localityDelay == -1)
      autoComputeLocalityDelay = true; // Compute from heartbeat interval

    if (LOG.isDebugEnabled()) {
      StringBuilder builder = new StringBuilder(HFSPScheduler.class.toString());
      // this.numSlotsForMapTrain = conf.getInt(TRAIN_MAP_SLOTS_KEYNAME, 0);
      // this.numSlotsForReduceTrain = conf.getInt(TRAIN_REDUCE_SLOTS_KEYNAME,
      // 0);
      // this.numMapSlotsForJob = conf.getInt(TRAINER_MIN_MAPS_KEYNAME, 2);
      // this.numReduceSlotsForJob = conf.getInt(TRAINER_MIN_REDUCES_KEYNAME,
      // 2);
      // this.eagerPreemptionEnabled
      builder.append(" initialized");
      if (this.mockMode) {
        builder.append(" in mockMode");
      }
      builder.append(" with configuration:").append("\t")
          .append("update interval: ").append(this.updateInterval).append("\t")
          .append("eager preemption: ").append(this.preemptionStrategy)
          .append("\t").append("delay enabled: ").append(this.delayEnabled)
          .append("\t").append("num slots for train map: ")
          .append(this.numSlotsForMapTrain).append("\t")
          .append("num slots for train reduce: ")
          .append(this.numSlotsForReduceTrain).append("\t")
          .append("min map tasks for train: ")
          .append(this.numMapTrainSlotsForJob).append("\t")
          .append("min reduce tasks for train: ")
          .append(this.numReduceTrainSlotsForJob).append("\t")
          .append("initial map task duration: ")
          .append(this.initialMapTaskDuration).append("\t")
          .append("initial reduce task duration: ")
          .append(this.initialReduceTaskDuration).append("\t")
          .append("duration modifier map: ").append(this.durationModifierMap)
          .append("\t").append("duration modifier reduce: ")
          .append(this.durationModifierReduce).append("\t")
          .append(" . Forcing the first update");

      LOG.debug(builder.toString());
    } else {
      LOG.info(HFSPScheduler.class + " initialized, forcing the first update");
    }

    // if (!this.mockMode)
    this.update();
  }

  private PreemptionStrategy loadPreemptionStrategyInstance(Configuration conf) {
    Class<? extends PreemptionStrategy> preemptionStrategyClass = conf
        .getClass(PREEMPTION_STRATEGY_CLASS_KEY, NoPreemption.class,
            PreemptionStrategy.class);
    return ReflectionUtils.newInstance(preemptionStrategyClass, conf);
  }

  // TODO: check everything is terminated correctly
  @Override
  public synchronized void terminate() throws IOException {
    // if (this.sizeBasedScheduler != null) {
    // this.sizeBasedScheduler.terminate();
    // }
    if (jobInProgressListener != null) {
      taskTrackerManager.removeJobInProgressListener(jobInProgressListener);
    }
    if (eagerTaskInitializationListener != null) {
      taskTrackerManager
          .removeJobInProgressListener(eagerTaskInitializationListener);
      eagerTaskInitializationListener.terminate();
    }

    if (this.updateThread != null)
      this.updateThread.terminate();

    super.terminate();
  }

  /** Update the scheduler status */
  synchronized void update() throws IOException {

    // Recompute locality delay from JobTracker heartbeat interval if enabled.
    // This will also lock the JT, so do it outside of a fair scheduler lock.
    if (!this.mockMode && this.delayEnabled && autoComputeLocalityDelay) {
      JobTracker jobTracker = (JobTracker) taskTrackerManager;
      localityDelay = Math.min(MAX_AUTOCOMPUTED_LOCALITY_DELAY,
          (long) (1.5 * jobTracker.getNextHeartbeatInterval()));
    }

    // update the simulation
    this.updateSimulation();

    // synchronize the number of slots in the simulation with the real one
    this.updateSlots();

    // update the trainer
    this.updateTrainer();

    // ask the trainer if there are jobs ready for size based scheduling
    this.updateJobQueues();
  }

  /**
   * Update the state of the job simulator using as interval the time between
   * the last event and the current system time. After that sort the map and
   * reduce queues
   */
  void updateSimulation() {
    long newLastEvent = this.clock.getTime();
    long time = newLastEvent - this.lastEvent;
    Interval interval = new Interval(time / 2);
    this.progressManager.update(interval);
    this.lastEvent = newLastEvent;
    this.cleanSizeBasedQueues();
    this.sortSizeBasedQueues();
  }

  /**
   * Set the number of slots of the virtual cluster to the new one
   * 
   * @param num
   *          number of new slots
   * @param type
   *          type of slots to set
   * @return false if the progress manager is not able to set the slot number
   */
  boolean setSlotsNum(final int num, final TaskType type) {
    boolean result = true;
    final int oldNum = this.cluster.getSlotsNum(type);
    if (oldNum != num) {
      updateSimulation();
      LOG.debug("Setting num of " + type.toString() + " slots from " + oldNum
          + " to " + num);
      result = this.progressManager.setSlotsNum(num, type);
    }
    return result;
  }

  /**
   * Update virtual cluster slots based on the real cluster slots
   * 
   * @return True if the update succeeded
   */
  boolean updateSlots() {
    boolean mapSuccess = this.setSlotsNum(this.getMaxTasks(TaskType.MAP),
        TaskType.MAP);
    boolean reduceSuccess = this.setSlotsNum(this.getMaxTasks(TaskType.REDUCE),
        TaskType.REDUCE);
    return mapSuccess && reduceSuccess;
  }

  void updateTrainer() {
    synchronized (this.trainer) {
      this.trainer.update(this.taskTrackerManager);
    }
  }

  /**
   * For each sub-job of each type, ask to the trainer if the job is ready to
   * end the training mode and enter the size-based mode
   * 
   * @throws IOException
   */
  void updateJobQueues() throws IOException {
    // LOG.debug("updateJobQueues " + this.getClock().getTime());
    for (TaskType type : TASK_TYPES) {
      Collection<JobInProgress> trainingJobs = this.getJobs(QueueType.TRAIN,
          type);
      // LOG.debug(type + " train jobs: " + trainingJobs.size());
      synchronized (trainingJobs) {
        Iterator<JobInProgress> iter = trainingJobs.iterator();
        while (iter.hasNext()) {
          JobInProgress jip = iter.next();
          if (this.isJobReadyForSizeBased(jip, type)) {
            LOG.debug("trainer sets " + jip.getJobID() + ":" + type
                + " ready => moving it from train queue to size based queue");
            iter.remove();
            this.addSizeBasedJob(jip, type, Phase.SIZE_BASED);
          }
        }
      }
    }
  }

  /**
   * @return whether the jip is ready for size based scheduling for that type
   */
  boolean isJobReadyForSizeBased(JobInProgress jip, TaskType type) {
    boolean isReady = false;
    synchronized (this.trainer) {
      isReady = this.trainer.isReady(jip, type);
    }
    return isReady;
  }

  @Override
  public List<Task> assignTasks(TaskTracker taskTracker) throws IOException {

    this.update();

    taskHelper.init(taskTracker.getStatus());

    // Update time waited for local maps for jobs skipped on last heartbeat
    if (this.delayEnabled)
      this.updateLocalityWaitTimes(taskHelper.currentTime);

    for (TaskType type : TASK_TYPES) {

      HelperForType helper = taskHelper.helper(type);

      if (!this.preemptionStrategy.isPreemptionActive()
          && helper.currAvailableSlots == 0) {
        // LOG.debug("assign(" + taskTracker.getTrackerName() + ", " + type
        // + "): no slots available");
        continue;
      }

      TreeSet<JobInProgress> trainJobs = new TreeSet<JobInProgress>(
          type == TaskType.MAP ? TRAIN_COMPARATOR_MAP : TRAIN_COMPARATOR_REDUCE);

      Collection<JobInProgress> trainJips = this.getJobs(QueueType.TRAIN, type);
      synchronized (trainJips) {
        trainJobs.addAll(trainJips);
      }

      TreeMap<JobDurationInfo, JobInProgress> sizeBasedJobs = new TreeMap<JobDurationInfo, JobInProgress>(
          JOB_DURATION_COMPARATOR);

      TreeMap<JobDurationInfo, JobInProgress> jobQueue = this
          .getSizeBasedJobQueue(type);
      synchronized (jobQueue) {
        sizeBasedJobs.putAll(jobQueue);
      }

      TreeMap<JobDurationInfo, TaskStatuses> taskStatusesSizeBased = helper.taskStatusesSizeBased;

      if (helper.doTrainScheduling) {
        assignTrainTasks(type, helper, trainJobs, sizeBasedJobs,
            taskStatusesSizeBased);
      }

      if (helper.doSizeBasedScheduling) {
        assignSizeBasedTasks(type, helper, sizeBasedJobs, taskStatusesSizeBased);
      }

    }

    if (LOG.isDebugEnabled()) {
      taskHelper.logInfos(LOG);
    }

    return (List<Task>) taskHelper.result.clone();
  }

  private void assignSizeBasedTasks(TaskType type, HelperForType helper,
      TreeMap<JobDurationInfo, JobInProgress> sizeBasedJobs,
      TreeMap<JobDurationInfo, TaskStatuses> taskStatusesSizeBased)
      throws IOException {

    final boolean isMap = type == TaskType.MAP;
    int totClaimedSlots = 0;

    // StringBuilder builder = new StringBuilder("SBJobs(");
    // builder.append(type).append("): [");
    // boolean first = true;
    // for (Entry<JobDurationInfo,JobInProgress> jip : sizeBasedJobs.entrySet())
    // {
    // if (first)
    // first = false;
    // else
    // builder.append(",");
    // builder.append(jip.getValue().getJobID())
    // .append(" -> ")
    // .append(jip.getKey().getPhaseDuration())
    // .append("/")
    // .append(jip.getKey().getPhaseTotalDuration())
    // .append(" p: ")
    // .append(this.getNumPendingNewTasks(jip.getValue(), type))
    // .append(" r: ")
    // .append(this.getNumRunningTasks(jip.getValue(), type))
    // .append(" f: ")
    // .append(this.getNumFinishedTasks(jip.getValue(), type));
    // }
    // builder.append("]");
    // LOG.debug(builder.toString());

    for (Entry<JobDurationInfo, JobInProgress> entry : sizeBasedJobs.entrySet()) {

      JobInProgress jip = entry.getValue();
      JobDurationInfo jdi = entry.getKey();
      TaskStatuses taskStatuses = taskStatusesSizeBased.get(jdi);

      if (!this.isJobReadyForTypeScheduling(jip, type)) {
        if (LOG.isDebugEnabled()
            && jip.getStatus().getRunState() != JobStatus.SUCCEEDED) {
          LOG.debug("SIZEBASED(" + jip.getJobID() + ":" + type + "):"
              + "job is not ready for scheduling (" + "status: "
              + JobStatus.getJobRunState(jip.getStatus().getRunState())
              + ", mapProgress: " + jip.getStatus().mapProgress()
              + ", reduceProgress: " + jip.getStatus().reduceProgress()
              + ", scheduleReduces: " + jip.scheduleReduces() + ")");
        }
        continue;
      }

      // NEW
      int pendingNewTasks = this.getNumPendingNewTasks(jip, type);
      int pendingResumableTasks = (taskStatuses == null) ? 0
          : taskStatuses.suspendedTaskStatuses.size();

      int totAvailableSizeBasedSlots = helper.totAvailableSizeBasedSlots();

      // missing slots for resumable
      int missingResumableSlots = 0;
      if (pendingResumableTasks > 0
          && pendingResumableTasks > totAvailableSizeBasedSlots) {
        if (totAvailableSizeBasedSlots <= 0)
          missingResumableSlots = pendingResumableTasks;
        else
          missingResumableSlots = pendingResumableTasks
              - totAvailableSizeBasedSlots;
        totAvailableSizeBasedSlots = (pendingResumableTasks > totAvailableSizeBasedSlots) ? 0
            : totAvailableSizeBasedSlots - pendingResumableTasks;
      }

      int missingNewSlots = 0;
      if (pendingNewTasks > 0 && pendingNewTasks > totAvailableSizeBasedSlots) {
        if (totAvailableSizeBasedSlots <= 0)
          missingNewSlots = pendingNewTasks;
        else
          missingNewSlots = pendingNewTasks - totAvailableSizeBasedSlots;
        totAvailableSizeBasedSlots = (pendingNewTasks > totAvailableSizeBasedSlots) ? 0
            : totAvailableSizeBasedSlots - pendingNewTasks;
      }

      TreeMap<TaskAttemptID, TaskStatus> suspended = null;
      if (taskStatuses != null)
        suspended = taskStatuses.suspendedTaskStatuses;

      if (pendingNewTasks > 0 || pendingResumableTasks > 0
          || (suspended != null && !suspended.isEmpty())) {
        LOG.debug(jip.getJobID()
            + ":"
            + type
            + " (d: "
            + jdi.getPhaseDuration()
            + "/"
            + jdi.getPhaseTotalDuration()
            + "):"
            + " pendingNewTasks: "
            + pendingNewTasks
            + " pendingResumableTasks: "
            + pendingResumableTasks
            // + " notResumableTasksOnThisTT: " + notResumableTasks
            + " totAvailableSizeBasedSlots: "
            + (helper.totAvailableSizeBasedSlots() <= 0 ? 0 : helper
                .totAvailableSizeBasedSlots()) + " currAvailableSlots: "
            + helper.currAvailableSlots + " => missingNewSlots: "
            + missingNewSlots + " missingResumableSlots: "
            + missingResumableSlots);
      }

      if (this.preemptionStrategy.isPreemptionActive()
          && (missingNewSlots > 0 || missingResumableSlots > 0)) {
        ClaimedSlots claimedSlots = this.claimSlots(helper,
            Phase.SIZE_BASED, jip, missingNewSlots, missingResumableSlots,
            totClaimedSlots, sizeBasedJobs, taskStatusesSizeBased);

        totClaimedSlots += claimedSlots.getNumPreemptedForNewTasks() 
                         + claimedSlots.getNumPreemptedForResumableTasks();

        LOG.debug(jip.getJobID() + " taskStatusesOnTT: "
            + taskStatusesSizeBased.get(jdi) + " pendingNewTasks: "
            + pendingNewTasks + " pendingResumableTasks: "
            + pendingResumableTasks + " missingNewSlots: " + missingNewSlots
            + " missingResumableSlots: " + missingResumableSlots);
      }

      while (pendingNewTasks > 0 || pendingResumableTasks > 0
          || (suspended != null && !suspended.isEmpty())) {

        if (helper.currAvailableSlots <= 0) {
          LOG.debug("SIZEBASED(" + jip.getJobID() + ":" + type + "):"
              + " no slots available on "
              + taskHelper.ttStatus.getTrackerName());
          return;
        }

        LOG.debug("SIZEBASED(" + jip.getJobID() + ":" + type + "):"
            + " totAvailableSizeBasedSlots(): "
            + helper.totAvailableSizeBasedSlots() + " pendingNewTasks: "
            + pendingNewTasks + " pendingResumableTasks: "
            + pendingResumableTasks + " suspended("
            + (suspended == null ? 0 : suspended.size()) + "): " + suspended);

        if (this.preemptionStrategy.isPreemptionActive()
            && (suspended != null && !suspended.isEmpty())) {
          TaskStatus toResume = suspended.remove(suspended.firstKey());
          // LOG.debug("RESUME: " + toResume.getTaskID() + " " +
          // toResume.getRunState());
          TaskAttemptID tAID = toResume.getTaskID();
          JobInProgress rJIP = this.taskTrackerManager.getJob(tAID.getTaskID()
              .getJobID());
          TaskInProgress tip = rJIP.getTaskInProgress(tAID.getTaskID());
          if (this.preemptionStrategy.resume(tip, toResume)) {
            taskHelper.resume(tAID, Phase.SIZE_BASED);
            pendingResumableTasks -= 1;
          } else {
            LOG.debug("SIZEBASED(" + jip.getJobID() + ":" + type + "):"
                + " cannot resume " + tAID + " on "
                + taskHelper.ttStatus.getTrackerName());
          }
        } else {

          Task task = this.obtainNewTask(jip, taskHelper.ttStatus, isMap,
              taskHelper.currentTime);

          if (task == null) {
            LOG.debug("SIZEBASED(" + jip.getJobID() + ":" + type + "):"
                + " cannot obtain slot for new task on "
                + taskHelper.ttStatus.getTrackerName() + " (#pendingNew: "
                + pendingNewTasks + ", #pendingResumable: "
                + pendingResumableTasks + ", #free_" + type + "_slots: "
                + helper.currAvailableSlots + ")");
            break;
          }

          taskHelper.slotObtained(task, Phase.SIZE_BASED);
          pendingNewTasks -= 1;
        }
      }
    }
  }

  private void assignTrainTasks(TaskType type, HelperForType helper,
      TreeSet<JobInProgress> trainJobs,
      TreeMap<JobDurationInfo, JobInProgress> sizeBasedJobs,
      TreeMap<JobDurationInfo, TaskStatuses> taskStatusesSizeBased)
      throws IOException {

    final boolean isMap = type == TaskType.MAP;

    /* #tasks running on other TTs that should be preempted */
    int totClaimedSlots = 0;
    for (JobInProgress jip : trainJobs) {

      if (!this.isJobReadyForTypeScheduling(jip, type)) {
        if (jip.getStatus().getRunState() != JobStatus.SUCCEEDED) {
          LOG.debug("TRAIN(" + jip.getJobID() + ":" + type + "): "
              + "job is not ready for scheduling (" + "status: "
              + JobStatus.getJobRunState(jip.getStatus().getRunState())
              + ", mapProgress: " + jip.getStatus().mapProgress()
              + ", reduceProgress: " + jip.getStatus().reduceProgress()
              + ", scheduleReduces: " + jip.scheduleReduces() + ")");
        }
        continue;
      }

      int runningTasks = this.getNumRunningTasks(jip, type);
      int finishedTasks = this.getNumFinishedTasks(jip, type);
      int trainTasksAssigned = runningTasks + finishedTasks;
      if (trainTasksAssigned >= helper.numTrainTasksForJob) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(jip.getJobID()
              + " has already obtained its training tasks (" + "runningTasks: "
              + runningTasks + " + finishedTasks: " + finishedTasks
              + " >= numTrainTasksForJob: " + helper.numTrainTasksForJob + ")");
        }
        continue;
      }

      int pendingNewTasks = this.getNumPendingNewTasks(jip, type);
      /* Train tasks remaining for jip */
      int trainTasksPending = Math.min(pendingNewTasks,
          helper.numTrainTasksForJob - trainTasksAssigned);

      int missingSlots = 0;
      if (trainTasksPending > 0) {
        if (helper.totAvailableTrainSlots() <= 0)
          missingSlots = trainTasksPending;
        else
          missingSlots = trainTasksPending - helper.totAvailableTrainSlots();
      }

      if (this.preemptionStrategy.isPreemptionActive() && missingSlots > 0) {
        ClaimedSlots claimedSlots = this.claimSlots(helper,
            Phase.TRAIN, jip, missingSlots, 0, totClaimedSlots, sizeBasedJobs,
            taskStatusesSizeBased);

        totClaimedSlots += claimedSlots.getNumPreemptedForNewTasks();
      }

      for (int i = 0; i < trainTasksPending && helper.currAvailableSlots > 0
          && helper.canAssignTrain(); i++) {
        Task task = this.obtainNewTask(jip, taskHelper.ttStatus, isMap,
            taskHelper.currentTime);

        if (task == null) {
          LOG.debug("TRAIN(" + jip.getJobID() + ":" + type + "):"
              + " cannot obtain slot for new task on "
              + taskHelper.ttStatus.getTrackerName());
          break;
        }

        taskHelper.slotObtained(task, Phase.TRAIN);
        pendingNewTasks--;
      }
    }

  }

  /**
   * Preempt missingSlots number of slots from jobs bigger
   * 
   * @param jip
   *          Job that clamies slots
   * @param allJobs
   *          all the size based jobs in the cluster
   * @param localJobs
   *          size based jobs that can be immediately suspended
   * @param missingSlots
   *          number of slots to claim
   * @param numToSkip
   *          number of slots on non-local
   * 
   * @return number of tasks preemped in the cluster for jip. The first elem of
   *         the tuple is the number of tasks preempted for new tasks, the
   *         second in the number of tasks preempted for tasks to be resumed
   */
  private ClaimedSlots claimSlots(HelperForType helper,
      final Phase phase, final JobInProgress jip, int missingNewSlots,
      int missingResumableSlots, int numToSkip,
      TreeMap<JobDurationInfo, JobInProgress> allJobs,
      TreeMap<JobDurationInfo, TaskStatuses> localJobs) {

    assert phase == Phase.SIZE_BASED || missingResumableSlots == 0;

    final TaskType type = helper.taskType;
    JobDurationInfo jdi = this.getDuration(jip.getJobID(), type);

    /* #size based tasks that occupies train slots in the cluster (suspendable) */
    int numTasksToPreempt = 0;
    if (phase == Phase.TRAIN) {
      /** num of size based tasks that can be suspended for training */
      int numOverflowSizeBasedTasks = helper.maxSizeBasedSlots > helper.runningSizeBasedTasks ? 0
          : helper.runningSizeBasedTasks - helper.maxSizeBasedSlots;

      /* num of size base tasks to preempt for the training of jip */
      numTasksToPreempt = Math.min(missingNewSlots, numOverflowSizeBasedTasks);
      if (LOG.isDebugEnabled()) {
        LOG.debug(phase.toString() + "(" + jip.getJobID() + ":" + type + "):"
            + " numOverflowSizeBasedTasks: " + numOverflowSizeBasedTasks
            + " numTasksToPreempt: " + numTasksToPreempt + " missingNewSlots: "
            + missingNewSlots + " numTrainTasksForJob: "
            + helper.numTrainTasksForJob + " canAssignTrain: "
            + helper.canAssignTrain() + " numToSkip: " + numToSkip);
      }
    } else {
      numTasksToPreempt = missingNewSlots;
      if (LOG.isDebugEnabled()) {
        LOG.debug(phase.toString() + "(" + jip.getJobID() + ":" + type + "):"
            + " missingNewSlots: " + missingNewSlots
            + " missingResumableSlots: " + missingResumableSlots
            + " numTrainTasksForJob: " + helper.numTrainTasksForJob
            + " canAssignTrain: " + helper.canAssignTrain() + " numToSkip: "
            + numToSkip);
      }
    }
    final int startingNumTasksToPreemptForNew = numTasksToPreempt;
    final int startingResumableSlots = missingResumableSlots;

    // try to free pendingTasks number of slots among running on this TT
    Iterator<Entry<JobDurationInfo, JobInProgress>> sizeBasedJobsDescIter = allJobs
        .descendingMap().entrySet().iterator();
    Iterator<Entry<JobDurationInfo, TaskStatuses>> sizeBasedJobsDescIterOnTT = localJobs
        .entrySet().iterator();

    Entry<JobDurationInfo, TaskStatuses> biggerOnTT = sizeBasedJobsDescIterOnTT
        .hasNext() ? sizeBasedJobsDescIterOnTT.next() : null;
    while (this.preemptionStrategy.isPreemptionActive()
        && (numTasksToPreempt > 0 || missingResumableSlots > 0)) {
      if (!sizeBasedJobsDescIter.hasNext()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(phase.toString() + "(" + jip.getJobID() + ":" + type + "):"
              + " should preempt " + numTasksToPreempt + " for new tasks and "
              + missingResumableSlots + " for resumable "
              + "tasks but no sizeBasedJob is running");
        }
        break;
      }

      Entry<JobDurationInfo, JobInProgress> nextSBJ = sizeBasedJobsDescIter
          .next();

      JobInProgress jipToPreempt = nextSBJ.getValue();

      /* don't try to suspend if jip is bigger than any other jip */
      if (jdi != null) {

        if (jipToPreempt.getJobID().equals(jip.getJobID())) {
          return new ClaimedSlots(startingNumTasksToPreemptForNew
              - numTasksToPreempt, startingResumableSlots
              - missingResumableSlots);
        }

        if (JOB_DURATION_COMPARATOR.compare(nextSBJ.getKey(), jdi) <= 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(phase.toString() + "(" + jip.getJobID() + ":" + type
                + "):" + " should preempt " + numTasksToPreempt
                + ", but bigger job avail is " + jip.getJobID() + ".len: "
                + jdi.getPhaseDuration() + " > "
                + nextSBJ.getValue().getJobID() + ".len: "
                + nextSBJ.getKey().getPhaseDuration());
          }
          return new ClaimedSlots(startingNumTasksToPreemptForNew
              - numTasksToPreempt, startingResumableSlots
              - missingResumableSlots);
        }
      }

      if (jipToPreempt.getJobID().equals(jip.getJobID())) {
        continue;
      }

      /*
       * don't try to claim slots from a job in training
       * 
       * FIXME: ideally a job can claim slots from a training job until this job
       * has enough tasks for training
       */
      if (!this.isTrained(jipToPreempt, type)) {
        LOG.debug(phase.toString() + "(" + jip.getJobID() + ":" + type + "):"
            + " ignoring " + jipToPreempt.getJobID() + " because in training");
        continue;
      }

      int numSuspendedOnThisTT = 0;

      /* if jipToPreempt has tasks on this TT, then suspend them */
      if (biggerOnTT != null // && type == TaskType.REDUCE
          && biggerOnTT.getKey().getJobID().equals(nextSBJ.getKey().getJobID())) {

        TreeMap<TaskAttemptID, TaskStatus> preemptableTAIDS = biggerOnTT
            .getValue().taskStatuses;
        int numPreemptions = Math.min(preemptableTAIDS.size(),
            missingResumableSlots + numTasksToPreempt);
        for (int i = 0; i < numPreemptions; i++) {
          TaskAttemptID pTAID = preemptableTAIDS.firstKey();
          TaskStatus pTS = preemptableTAIDS.remove(pTAID);
          JobInProgress pJIP = this.taskTrackerManager.getJob(pTAID.getJobID());
          TaskInProgress pTIP = pJIP.getTaskInProgress(pTAID.getTaskID());

          if (type == TaskType.REDUCE) {
            // if (this.eagerPreemption == PreemptionType.KILL
            // && pTIP.killTask(pTAID, false)) {
            // if (missingResumableSlots > 0)
            // missingResumableSlots -= 1;
            // else
            // numTasksToPreempt -= 1;
            // numSuspendedOnThisTT += 1;
            // if (jdi == null) {
            // taskHelper.kill(pTAID, jip.getJobID(), phase);
            // } else {
            // taskHelper.kill(pTAID, jip.getJobID(), phase, nextSBJ.getKey(),
            // jdi);
            // }
            // } else if (this.preemptionStrategy.isPreemptionActive()
            // && this.canBeSuspended(pTS) && pTIP.suspendTaskAttempt(pTAID)) {
            if (this.preemptionStrategy.isPreemptionActive()
                && this.preemptionStrategy.canBePreempted(pTS)
                && this.preemptionStrategy.preempt(pTIP, pTS)) {
              if (missingResumableSlots > 0)
                missingResumableSlots -= 1;
              else
                numTasksToPreempt -= 1;
              numSuspendedOnThisTT += 1;
              if (jdi == null) {
                taskHelper.suspend(pTAID, jip.getJobID(), phase);
              } else {
                taskHelper.suspend(pTAID, jip.getJobID(), phase,
                    nextSBJ.getKey(), jdi);
              }
            } else {
              LOG.debug(phase.toString() + "(" + jip.getJobID() + ":" + type
                  + "): cannot suspend " + pTAID + " for " + jip);
            }
          }
        }

        if (preemptableTAIDS.size() - numPreemptions <= 0) {
          biggerOnTT = sizeBasedJobsDescIterOnTT.hasNext() ? sizeBasedJobsDescIterOnTT
              .next() : null;
        }
      }

      /* #tasks that can be preempted */
      int numPreemptibleRunTasks = this.getNumRunningTasks(jipToPreempt, type)
          - numSuspendedOnThisTT;

      /*
       * Two cases: numToSkip is bigger then preemptible tasks or it is not: -
       * is bigger: then we skip this preemptible jip - it is not: then
       * numToSkip is set to 0 and we do the real wait preemption
       */
      if (numPreemptibleRunTasks <= numToSkip) {
        numToSkip -= numPreemptibleRunTasks;
      } else {
        /* #tasks that can be preempted by jip */
        int numPreemptibleByJIPRunTasks = numPreemptibleRunTasks - numToSkip;

        numToSkip = 0;

        /* #tasks that will be preempted by jip on other TTs s */
        int numRunTasksEventuallyPreemptedByJIP = Math.min(numTasksToPreempt,
            numPreemptibleByJIPRunTasks);

        numTasksToPreempt -= numRunTasksEventuallyPreemptedByJIP;
      }
    }

    return new ClaimedSlots(startingNumTasksToPreemptForNew
        - numTasksToPreempt, startingResumableSlots - missingNewSlots);
  }

  /**
   * Add the given jip to the trainingTypeQueue and to the trainer followed list
   */
  void addTrainJob(JobInProgress jip, TaskType type) {
    if (type == TaskType.MAP) {
      this.trainingMapJobs.add(jip);
    } else {
      this.trainingReduceJobs.add(jip);
    }
    this.trainer.followJob(jip, type);
    LOG.debug("add " + jip.getJobID() + ":" + type
        + " to train queue and set followed by trainer");

    this.infos.put(jip.getJobID(), new JobInfo());

    this.addSizeBasedJob(jip, type, Phase.TRAIN);

    try {
      this.update();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Add the given jip to the sizeBasedTypeQueue with the related duration.
   */
  boolean addSizeBasedJob(JobInProgress jip, TaskType type, Phase phase) {
    boolean isMap = type == TaskType.MAP;
    Map<JobDurationInfo, JobInProgress> sizeBasedJobs = isMap ? this.sizeBasedMapJobsQueue
        : this.sizeBasedReduceJobsQueue;
    Map<JobID, JobDurationInfo> jIDToJDI = isMap ? this.jIDToMapJDI
        : this.jIDToReduceJDI;
    JobDurationInfo newJDI = this.createJobDurationInfo(jip, type, phase);

    boolean res = (phase == Phase.TRAIN) ? this.progressManager
        .addJobDurationInfo(newJDI) : this.progressManager
        .updateJobDurationInfo(newJDI);
    if (!res) {
      if (phase == Phase.TRAIN) {
        LOG.error("cannot add size " + newJDI.toString() + " for "
            + jip.getJobID() + ":" + type);
      } else if (phase == Phase.SIZE_BASED) {
        LOG.error("cannot update size for " + jip.getJobID() + ":" + type
            + " from " + jIDToJDI.get(jip.getJobID()) + " to " + newJDI);
      }
      return false;
    }

    JobDurationInfo prevJDI = jIDToJDI.put(jip.getJobID(), newJDI);
    if (prevJDI != null) {
      // TODO FIXME: why can't use sizeBasedJobs.remove(prevJDI)?
      Iterator<JobDurationInfo> iter = sizeBasedJobs.keySet().iterator();
      while (iter.hasNext()) {
        JobDurationInfo next = iter.next();
        if (next.getJobID().equals(prevJDI.getJobID()))
          iter.remove();
      }
    }
    sizeBasedJobs.put(newJDI, jip);

    if (LOG.isDebugEnabled()) { // TODO: deleteme
      HashMap<JobID, JobDurationInfo> jdis = new HashMap<JobID, JobDurationInfo>();
      for (Entry<JobDurationInfo, JobInProgress> entry : sizeBasedJobs
          .entrySet()) {
        JobDurationInfo jdi = entry.getKey();
        assert !jdis.containsKey(jdi.getJobID()) : String
            .format("%s %s %s", jdi.getJobID(), jdis.get(jdi.getJobID())
                .getJobID(), jdi.getJobID());
        jdis.put(jdi.getJobID(), jdi);
      }
    }

    this.sortSizeBasedQueue(type);

    LOG.info("UPDATE_SIZE " + jip.getJobID() + ":" + type + " "
        + (prevJDI == null ? "None" : prevJDI) + " -> " + newJDI);

    return true;
  }

  private TreeMap<JobDurationInfo, JobInProgress> getSizeBasedJobQueue(
      TaskType type) {
    return type == TaskType.MAP ? this.sizeBasedMapJobsQueue
        : this.sizeBasedReduceJobsQueue;
  }

  private Task obtainNewTask(JobInProgress job, TaskTrackerStatus tts,
      boolean isMap, long currentTime) throws IOException {
    TaskTrackerManager ttm = this.taskTrackerManager;
    ClusterStatus clusterStatus = this.taskTrackerManager.getClusterStatus();
    int numTaskTrackers = clusterStatus.getTaskTrackers();
    Task task = null;
    if (isMap) {
      LocalityLevel localityLevel = this.getAllowedLocalityLevel(job,
          currentTime);
      switch (localityLevel) {
        case NODE:
          task = job.obtainNewNodeLocalMapTask(tts, numTaskTrackers,
              ttm.getNumberOfUniqueHosts());
          break;
        case RACK:
          task = job.obtainNewNodeOrRackLocalMapTask(tts, numTaskTrackers,
              ttm.getNumberOfUniqueHosts());
          break;
        default:
          task = job.obtainNewMapTask(tts, numTaskTrackers,
              ttm.getNumberOfUniqueHosts());
          break;
      }
    } else {
      task = job.obtainNewReduceTask(tts, numTaskTrackers,
          ttm.getNumberOfUniqueHosts());
    }

    if (this.delayEnabled && isMap && task != null) {
      this.updateLastMapLocalityLevel(job, task, tts);
    }
    return task;
  }

  /**
   * Update locality wait times for jobs that were skipped at last heartbeat.
   */
  private void updateLocalityWaitTimes(long currentTime) {
    long timeSinceLastHeartbeat = (lastHeartbeatTime == 0 ? 0 : currentTime
        - lastHeartbeatTime);
    lastHeartbeatTime = currentTime;
    for (JobInfo info : infos.values()) {
      if (info.skippedAtLastHeartbeat) {
        info.timeWaitedForLocalMap += timeSinceLastHeartbeat;
        info.skippedAtLastHeartbeat = false;
      }
    }
  }

  /**
   * Update a job's locality level and locality wait variables given that that
   * it has just launched a map task on a given task tracker.
   */
  private void updateLastMapLocalityLevel(JobInProgress job,
      Task mapTaskLaunched, TaskTrackerStatus tracker) {
    JobInfo info = infos.get(job.getJobID());
    LocalityLevel localityLevel = LocalityLevel.fromTask(job, mapTaskLaunched,
        tracker);
    info.lastMapLocalityLevel = localityLevel;
    info.timeWaitedForLocalMap = 0;
    LOG.debug(job.getJobID() + " set lastLocalityLevel="
        + info.lastMapLocalityLevel + " timeWaitedForLocalMap=" + 0);
  }

  /**
   * Get the maximum locality level at which a given job is allowed to launch
   * tasks, based on how long it has been waiting for local tasks. This is used
   * to implement the "delay scheduling" feature of the Fair Scheduler for
   * optimizing data locality. If the job has no locality information (e.g. it
   * does not use HDFS), this method returns LocalityLevel.ANY, allowing tasks
   * at any level. Otherwise, the job can only launch tasks at its current
   * locality level or lower, unless it has waited at least localityDelay
   * milliseconds (in which case it can go one level beyond) or 2 *
   * localityDelay millis (in which case it can go to any level).
   */
  protected LocalityLevel getAllowedLocalityLevel(JobInProgress job,
      long currentTime) {
    if (!this.delayEnabled) {
      return LocalityLevel.ANY;
    }

    JobInfo info = infos.get(job.getJobID());
    if (info == null) { // Job not in infos (shouldn't happen)
      LOG.error("getAllowedLocalityLevel called on job " + job
          + ", which does not have a JobInfo in infos");
      return LocalityLevel.ANY;
    }
    if (job.nonLocalMaps.size() > 0) { // Job doesn't have locality information
      LOG.debug(job.getJobID() + " doesn't have locality information ("
          + "job.nonLocalMaps.size() > 0)");
      return LocalityLevel.ANY;
    }

    // In the common case, compute locality level based on time waited
    switch (info.lastMapLocalityLevel) {
      case NODE: // Last task launched was node-local
        if (info.timeWaitedForLocalMap >= 2 * localityDelay) {
          LOG.debug(job.getJobID() + " lastMapLocalityLevel: "
              + info.lastMapLocalityLevel + " timeWaitedForLocalMap: "
              + info.timeWaitedForLocalMap + " >= 2 * localityDelay:"
              + localityDelay + " => locality level is ANY");
          return LocalityLevel.ANY;
        } else if (info.timeWaitedForLocalMap >= localityDelay) {
          LOG.debug(job.getJobID() + " lastMapLocalityLevel: "
              + info.lastMapLocalityLevel + " timeWaitedForLocalMap: "
              + info.timeWaitedForLocalMap + " >= localityDelay:"
              + localityDelay + " => locality level is RACK");
          return LocalityLevel.RACK;
        } else {
          LOG.debug(job.getJobID() + " lastMapLocalityLevel: "
              + info.lastMapLocalityLevel + " timeWaitedForLocalMap: "
              + info.timeWaitedForLocalMap + " < localityDelay:"
              + localityDelay + " => locality level is NODE");
          return LocalityLevel.NODE;
        }
      case RACK: // Last task launched was rack-local
        if (info.timeWaitedForLocalMap >= localityDelay) {
          LOG.debug(job.getJobID() + " lastMapLocalityLevel: "
              + info.lastMapLocalityLevel + " timeWaitedForLocalMap: "
              + info.timeWaitedForLocalMap + " >= localityDelay:"
              + localityDelay + " => locality level is ANY");
          return LocalityLevel.ANY;
        } else {
          LOG.debug(job.getJobID() + " lastMapLocalityLevel: "
              + info.lastMapLocalityLevel + " timeWaitedForLocalMap: "
              + info.timeWaitedForLocalMap + " < localityDelay:"
              + localityDelay + " => locality level is RACK");
          return LocalityLevel.RACK;
        }
      default: // Last task was non-local; can launch anywhere
        LOG.debug(job.getJobID() + " lastMapLocalityLevel: "
            + info.lastMapLocalityLevel + " => locality level is ANY");
        return LocalityLevel.ANY;
    }
  }

  /** Number of pending tasks without suspended tasks */
  int getNumPendingNewTasks(JobInProgress jip, TaskType type) {
    return this.getNumPendingTasks(jip, type)
        - this.preemptionStrategy.getPreemptedTasks(jip, type);
  }

  /** Number of pending and suspended tasks */
  int getNumPendingTasks(JobInProgress jip, TaskType type) {
    return type == TaskType.MAP ? jip.pendingMaps() : jip.pendingReduces();
  }

  int getNumDesiredTasks(JobInProgress job, TaskType type) {
    return type == TaskType.MAP ? job.desiredMaps() : job.desiredReduces();
  }

  /** running tasks (with suspended tasks) */
  int getNumRunningTasks(JobInProgress job, TaskType type) {
    return type == TaskType.MAP ? job.runningMaps() : job.runningReduces();
  }

  int getNumFinishedTasks(JobInProgress job, TaskType type) {
    return type == TaskType.MAP ? job.finishedMaps() : job.finishedReduces();
  }

  int getNumRunningTasksNotSuspended(QueueType schedulerType, TaskType taskType) {
    Collection<JobInProgress> jobs = this.getJobs(schedulerType, taskType);
    int acc = 0;
    synchronized (jobs) {
      for (JobInProgress job : jobs) {
        acc += this.getNumRunningTasks(job, taskType)
        // - this.getNumSuspendedTasks(job, taskType);
            - this.preemptionStrategy.getPreemptedTasks(job, taskType);
      }
    }

    if (acc < 0)
      return 0;
    return acc;
  }

  /** If there are jobs of the specified type for the specified scheduler */
  public boolean isSchedulingActive(QueueType schedulerType, TaskType taskType) {
    Collection<JobInProgress> jobs = this.getJobs(schedulerType, taskType);
    synchronized (jobs) {
      for (JobInProgress job : jobs) {
        if (this.getNumPendingTasks(job, taskType) > 0)
          return true;
      }
    }
    return false;
  }

  /**
   * @return true if the job is ready to obtain slots for tasks of the given
   *         type
   */
  private boolean isJobReadyForTypeScheduling(JobInProgress jip, TaskType type) {
    return jip.getStatus().getRunState() == JobStatus.RUNNING
        && (type == TaskType.MAP || jip.scheduleReduces());
  }

  /** All the jobs managed by the scheduler */
  @Override
  public Collection<JobInProgress> getJobs(String queueName) {
    return this.jIDToJIP.values();
  }

  /** Jobs in a particular queue */
  public Collection<JobInProgress> getJobs(QueueType schedulerType,
      TaskType taskType) {
    if (schedulerType == QueueType.TRAIN) {
      if (taskType == TaskType.MAP) {
        return this.trainingMapJobs;
      } else {
        return this.trainingReduceJobs;
      }
    } else if (schedulerType == QueueType.SIZE_BASED) {
      if (taskType == TaskType.MAP) {
        return this.sizeBasedMapJobsQueue.values();
      } else {
        return this.sizeBasedReduceJobsQueue.values();
      }
    }

    return new HashSet<JobInProgress>();
  }

  /** Find a job in a particular queue */
  public JobInProgress getJob(JobID jobID, QueueType schedulerType,
      TaskType taskType) {
    Collection<JobInProgress> jobs = this.getJobs(schedulerType, taskType);
    synchronized (jobs) {
      for (JobInProgress job : jobs) {
        if (job.getJobID().equals(jobID))
          return job;
      }
    }

    return null;
  }

  /** Return the duration of a job or null if it doesn't exist */
  public JobDurationInfo getDuration(JobID jobID, TaskType type) {
    return (type == TaskType.MAP ? this.jIDToMapJDI : this.jIDToReduceJDI)
        .get(jobID);
  }

  public JobDurationInfo createJobDurationInfo(JobInProgress jip,
      TaskType type, Phase phase) {
    if (phase.equals(Phase.TRAIN)) {
      return this.createInitialJobDurationInfo(jip, type);
    }
    if (phase.equals(Phase.SIZE_BASED)) {
      return this.trainer.getJobDurationInfo(jip, type);
    }
    return null;
  }

  /** Create a first estimation of the job duration based on available data */
  private JobDurationInfo createInitialJobDurationInfo(JobInProgress jip,
      TaskType type) {

    TreeMap<JobDurationInfo, JobInProgress> sizeBasedJobQueue = this
        .getSizeBasedJobQueue(type);

    long singleTaskDuration = 0;

    if (!sizeBasedJobQueue.isEmpty()) {
      long taskDuration = 0;
      int trained = 0;
      for (Entry<JobDurationInfo, JobInProgress> entry : sizeBasedJobQueue
          .entrySet()) {
        if (this.isTrained(entry.getValue(), type)) {
          JobDurationInfo jdi = entry.getKey();
          int numVirtualTasks = jdi.getTasks().size();
          if (entry.getKey().getPhaseTotalDuration() > 0 && numVirtualTasks > 0) {
            trained += 1;
            taskDuration += Math.ceil(entry.getKey().getPhaseTotalDuration()
                / numVirtualTasks);
          }
        }
      }

      if (trained > 0) {
        singleTaskDuration = (long) Math.ceil(taskDuration / trained);
      }
    }

    if (singleTaskDuration == 0) {
      singleTaskDuration = (type == TaskType.MAP) ? initialMapTaskDuration
          : initialReduceTaskDuration;
    }

    float durationModifier = (type == TaskType.MAP) ? this.durationModifierMap
        : this.durationModifierReduce;

    LOG.debug(jip.getJobID() + ":" + type + " singleTaskDuration: "
        + singleTaskDuration + " durationModifier: " + durationModifier
        + " => singleTaskDuration: "
        + (long) (singleTaskDuration * durationModifier));

    singleTaskDuration = (long) (singleTaskDuration * durationModifier);

    return new UniformJobDurationInfo(jip, singleTaskDuration, type);
  }

  /** If the job has finished the train phase */
  private boolean isTrained(JobInProgress jip, TaskType type) {
    return !(type == TaskType.MAP ? this.trainingMapJobs
        : this.trainingReduceJobs).contains(jip);
  }

  public Clock getClock() {
    return this.clock;
  }

  /**
   * Sort job durations queues based on the current value
   * 
   * @param type
   */
  private void sortSizeBasedQueue(TaskType type) {
    TreeMap<JobDurationInfo, JobInProgress> newQueue = new TreeMap<JobDurationInfo, JobInProgress>(
        HFSPScheduler.JOB_DURATION_COMPARATOR);
    Map<JobDurationInfo, JobInProgress> oldQueue = this
        .getSizeBasedJobQueue(type);

    if (LOG.isDebugEnabled()) { // TODO: deleteme
      HashMap<JobID, JobDurationInfo> jdis = new HashMap<JobID, JobDurationInfo>();
      for (Entry<JobDurationInfo, JobInProgress> entry : oldQueue.entrySet()) {
        JobDurationInfo jdi = entry.getKey();
        assert !jdis.containsKey(jdi.getJobID()) : String.format("%s %s %s",
            jdi.getJobID(), jdis.get(jdi.getJobID()), jdi);
        jdis.put(jdi.getJobID(), jdi);
      }
    }

    int oldSize = oldQueue.size();
    synchronized (oldQueue) {
      newQueue.putAll(oldQueue);
      oldQueue.clear();

      // FIXME: putAll not working with comparator, don't know why
      for (Entry<JobDurationInfo, JobInProgress> entry : newQueue.entrySet()) {
        oldQueue.put(entry.getKey(), entry.getValue());
      }
    }
    assert oldSize == oldQueue.size() : String.format(
        "oldSize: %s newSize: %s", oldSize, oldQueue.size());

    // if (LOG.isDebugEnabled()) {
    // StringBuilder builder = new StringBuilder("time update on " +
    // "SizeBasedQueue(").append(type).append( "): [");
    // boolean first = true;
    // for (Entry<JobDurationInfo, JobInProgress> entry : oldQueue.entrySet()) {
    // if (first)
    // first = false;
    // else
    // builder.append(", ");
    // builder.append(entry.getKey().getPhaseDuration())
    // .append(" -> ").append(entry.getValue().getJobID());
    // }
    // builder.append("]");
    // LOG.debug(builder.toString());
    // }
  }

  /**
   * Sort size based job queues based on job durations
   */
  private void sortSizeBasedQueues() {
    this.sortSizeBasedQueue(TaskType.MAP);
    this.sortSizeBasedQueue(TaskType.REDUCE);
  }

  /**
   * Try to remove the jobs
   */
  private void cleanSizeBasedQueues() {
    for (JobInProgress jip : this.jIDToJIP.values()) {
      this.removeJobIfCompleted(jip);
    }
  }

  /**
   * Get the maximum map and reduce tasks for the cluster
   * 
   * @see ClusterStatus#getMaxMapTasks()
   * @see ClusterStatus#getMaxReduceTasks()
   */
  int getMaxTasks(TaskType type) {
    ClusterStatus status = this.taskTrackerManager.getClusterStatus();
    return type == TaskType.MAP ? status.getMaxMapTasks() : status
        .getMaxReduceTasks();
  }

  public static void main(String[] args) throws IllegalArgumentException,
      InstantiationException, IllegalAccessException,
      InvocationTargetException, javax.xml.parsers.ParserConfigurationException, IOException {
    HFSPScheduler scheduler = new HFSPScheduler();
    ConfigurationDescriptionToXMLConverter converter = ConfigurationDescriptionToXMLConverter
        .newInstance();
    scheduler.accept(converter);
    if (args.length > 0) {
      FileOutputStream outputStream = new FileOutputStream(new File(args[0]).getAbsoluteFile());
      converter.write(outputStream);
      outputStream.close();
    } else {
      converter.write(System.out);
    }
  }

  public void accept(ConfigurationDescriptionToXMLConverter converter) {
    this.configurationManager.accept(converter);
    this.trainer.accept(converter);
  }

  /**
   * Remove a job if it is completed in both the real and the virtual cluster
   */
  public boolean removeJobIfCompleted(JobInProgress jip) {
    if (!jip.isComplete()) {
      if (LOG.isDebugEnabled()) {
        JobStatus jobStatus = jip.getStatus();
        LOG.debug("Cannot remove " + jip + " because is not completed "
            + "(mapProgress: " + jobStatus.mapProgress() + ","
            + "reduceProgress: " + jobStatus.reduceProgress() + ")");
      }
      return false;
    }
    JobID jid = jip.getJobID();
    JobDurationInfo mapDuration = this.getDuration(jid, TaskType.MAP);
    if (!mapDuration.isFinished()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot remove " + jip + " because it's map virtual "
            + "time is not 0 (mapVirtualTime: " + mapDuration + ")");
      }
      return false;
    }

    JobDurationInfo reduceDuration = this.getDuration(jid, TaskType.REDUCE);
    if (!reduceDuration.isFinished()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot remove " + jip + " because it's reduce virtual "
            + "time is not 0 (reduceVirtualTime: " + reduceDuration + ")");
      }
      return false;
    }

    if (this.jIDToMapJDI.containsKey(jid)) {
      this.jIDToMapJDI.remove(jid);
    }
    if (this.jIDToReduceJDI.containsKey(jid)) {
      this.jIDToReduceJDI.remove(jid);
    }
    if (this.jIDToJIP.containsKey(jid)) {
      this.jIDToJIP.remove(jid);
    }
    if (this.infos.containsKey(jid)) {
      this.infos.remove(jid);
    }
    if (this.sizeBasedMapJobsQueue.containsKey(mapDuration)) {
      this.sizeBasedMapJobsQueue.remove(mapDuration);
    }
    if (this.sizeBasedReduceJobsQueue.containsKey(reduceDuration)) {
      this.sizeBasedReduceJobsQueue.remove(reduceDuration);
    }
    if (this.trainingMapJobs.contains(jip)) {
      this.trainingMapJobs.remove(jip);
    }
    if (this.trainingReduceJobs.contains(jip)) {
      this.trainingReduceJobs.remove(jip);
    }

    return true;
  }
}

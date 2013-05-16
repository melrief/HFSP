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
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.mapreduce.TaskType;

public class VirtualCluster<TaskDurationInfoClass extends TaskDurationInfoBase> {

  private List<TaskDurationInfoClass> mapSlots;
  private List<TaskDurationInfoClass> reduceSlots;

  public VirtualCluster(final int maps, final int reduces) {
    this.mapSlots = new ArrayList<TaskDurationInfoClass>(maps);
    for (int i = 0; i < maps; i++) {
      this.mapSlots.add(null);
    }
    this.reduceSlots = new ArrayList<TaskDurationInfoClass>(reduces);
    for (int i = 0; i < reduces; i++) {
      this.reduceSlots.add(null);
    }
  }

  public final List<TaskDurationInfoClass> getSlots(final TaskType type) {
    if (type == TaskType.MAP) {
      return getMapSlots();
    } else {
      return getReduceSlots();
    }
  }

  public final void freeSlot(final TaskType type, final int index) {
    List<TaskDurationInfoClass> slots = getSlots(type);
    if (slots.get(index) != null) {
      slots.get(index).setRunning(false);
    }
    slots.set(index, null);
  }

  public final void assignSlot(final TaskType type, final int index,
      final TaskDurationInfoClass task) {
    getSlots(type).set(index, task);
    task.setRunning(true);
  }

  /**
   * Change the number of slots in the cluster for that particular task type.
   * Note that if num is smaller than actual size then this function works only
   * if there are enough slots to be deleted that are not used for tasks. In
   * case there aren't enough slots this method removes all those who can.
   * 
   * 
   * @param num
   *          the new number of slots
   * @param type
   *          the task type
   * @return true if successful
   */
  public final boolean setSlotsNum(final int num, final TaskType type) {
    List<TaskDurationInfoClass> slots = this.getSlots(type);
    int newNum = slots.size() - num;
    if (newNum > 0) { // must delete newNum slots, from the beginning
      ListIterator<TaskDurationInfoClass> iter = slots.listIterator();
      while (iter.hasNext() && newNum > 0) {
        TaskDurationInfoClass assigned = iter.next();
        if (assigned == null) {
          iter.remove();
          newNum -= 1;
        }
      }
      return num <= 0;
    }
    if (newNum < 0) {
      for (; newNum < 0; newNum++) { // must add newNum slots
        slots.add(null);
      }
    }
    return true;
  }

  public final int getSlotsNum(final TaskType type) {
    return this.getSlots(type).size();
  }

  public final List<TaskDurationInfoClass> getMapSlots() {
    return mapSlots;
  }

  public final List<TaskDurationInfoClass> getReduceSlots() {
    return reduceSlots;
  }

}

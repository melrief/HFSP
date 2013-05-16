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

import java.util.Comparator;

/**
 * Duration comparison between two different jobs.
 * 
 */
public class JobDurationComparator implements Comparator<JobDurationInfo> {

  /**
   * Compare informations from two jobs based on the duration.
   */
  @Override
  public int compare(final JobDurationInfo info1, final JobDurationInfo info2) {
    if (info1.getJobID().equals(info2.getJobID()))
      return 0;

    Long duration1 = info1.getPhaseDuration();
    Long duration2 = info2.getPhaseDuration();

    int durationComparation = duration1.compareTo(duration2);

    if (durationComparation != 0) {
      return durationComparation;
    } else if (info1.hasFinishPosition() && info2.hasFinishPosition()) {
      int posComparation = info1.getFinishPosition()
          - info2.getFinishPosition();
      assert posComparation != 0;
      return posComparation < 0 ? -1 : +1;
    } else {
      return info1.getJobID().compareTo(info2.getJobID());
    }
  }

}

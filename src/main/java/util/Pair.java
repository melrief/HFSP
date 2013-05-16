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
package util;

public class Pair<T1, T2> {

    private T2 t2;
    private T1 t1;

    public Pair(final T1 t1, final T2 t2) {
        this.setT1(t1);
        this.setT2(t2);
    }

    public T1 getT1() {
        return t1;
    }

    public void setT1(final T1 t1) {
        this.t1 = t1;
    }

    public T2 getT2() {
        return t2;
    }

    public void setT2(final T2 t2) {
        this.t2 = t2;
    }

    @Override
    public boolean equals(final Object obj) {
        try {
            @SuppressWarnings("unchecked")
            Pair<T1, T2> casted = (Pair<T1, T2>) obj;
            return this.getT1() == casted.getT1()
                && this.getT2() == casted.getT2();
        } catch (ClassCastException e) {
            return false;
        }
    }

    @Override
    public String toString() {
        return  "(" + this.getT1().toString() + ", "
              + this.getT2() + ")";
    }
    
    
}

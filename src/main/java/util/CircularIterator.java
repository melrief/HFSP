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

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class CircularIterator<E> implements Iterator<E> {

    private Collection<E> collection;
    private Iterator<E> iterator;

    public CircularIterator(final Collection<E> collection) {
        this.collection = collection;
        this.iterator = collection.iterator();
    }

    @Override
    public final boolean hasNext() {
        if (collection.size() <= 0) {
            return false;
        }
        return true;
    }

    @Override
    public final E next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        if (!iterator.hasNext()) {
            iterator = collection.iterator();
        }
        return iterator.next();
    }

    @Override
    public final void remove() {
        iterator.remove();
    }

}

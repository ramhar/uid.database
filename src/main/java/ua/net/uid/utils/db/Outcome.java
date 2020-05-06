/*
 * Copyright 2020 nightfall.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ua.net.uid.utils.db;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface Outcome<T> {
    Outcome<Boolean> NOT_EMPTY = ResultSet::next;
    Outcome<Integer> UPDATES_COUNT = result -> result.getStatement().getUpdateCount();
    Outcome<Integer> FIRST_INT = result -> result.next() ? result.getInt(1) : null;
    Outcome<Long> FIRST_LONG = result -> result.next() ? result.getLong(1) : null;
    Outcome<Object> FIRST_OBJECT = result -> result.next() ? result.getObject(1) : null;

    T process(final ResultSet result) throws SQLException;
}

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

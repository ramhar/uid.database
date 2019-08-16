package ua.net.uid.utils.db;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface Fetcher<T> {
    T fetch(final ResultSet result) throws SQLException;
}

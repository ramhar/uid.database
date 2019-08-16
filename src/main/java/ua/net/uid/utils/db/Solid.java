package ua.net.uid.utils.db;

import java.sql.SQLException;

public interface Solid {
    void execute(Transaction db) throws SQLException;
}

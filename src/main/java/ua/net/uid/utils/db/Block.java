package ua.net.uid.utils.db;

import java.sql.SQLException;

public interface Block {
    void execute(Session db) throws SQLException;
}

package ua.net.uid.utils.db;

import java.sql.Connection;
import java.sql.SQLException;

public final class Transaction extends Session {
    private final Connection connection;

    Transaction(final Connection connection) {
        this.connection = connection;
    }

    @Override
    protected Connection connection() {
        return connection;
    }

    @Override
    protected void release(Connection connection) {
    }

    public void commit() throws SQLException {
        connection.commit();
    }

    public void rollback() throws SQLException {
        connection.rollback();
    }
}

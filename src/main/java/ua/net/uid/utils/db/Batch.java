package ua.net.uid.utils.db;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class Batch implements Closeable {
    private final Session session;
    private final String query;
    private Connection connection = null;
    private PreparedStatement statement = null;

    Batch(Session session, String query) {
        this.session = session;
        this.query = query;
    }

    private Connection connection() throws SQLException {
        if (connection == null)
            connection = session.connection();
        return connection;
    }

    private PreparedStatement statement() throws SQLException {
        if (statement == null) {
            try {
                statement = connection().prepareStatement(query);
            } catch (SQLException ex) {
                close();
                throw ex;
            }
        }
        return statement;
    }

    public Batch values(Object... params) throws SQLException {
        if (params != null) {
            try {
                final PreparedStatement stmt = statement();
                Processor.bind(stmt, params);
                stmt.addBatch();
            } catch (SQLException ex) {
                close();
                throw ex;
            }
        }
        return this;
    }

    public int[] execute() throws SQLException {
        try {
            return statement.executeBatch();
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        if (statement != null) {
            try { statement.close(); } catch (final SQLException ignored) {}
        }
        statement = null;
        if (connection != null) {
            session.release(connection);
            connection = null;
        }
    }
}

package ua.net.uid.utils.db;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public final class Database extends Session {
    private final Connector connector;

    public Database(Connector connector) {
        this.connector = connector;
    }

    public Database(DataSource source, boolean pooled) {
        this(pooled ? new Connector.Pooled(source) : new Connector.Source(source));
    }

    public Database(DataSource source) {
        this(source, (source instanceof ConnectionPoolDataSource));
    }

    public Database(Connection connection) {
        this(new Connector.Static(connection));
    }

    @Override
    protected Connection connection() throws SQLException {
        return connector.get();
    }

    @Override
    protected void release(Connection connection) {
        connector.release(connection);
    }

    public void transaction(Solid tx) throws SQLException {
        Connection connection = null;
        try {
            connection = connection();
            connection.setAutoCommit(false);
            tx.execute(new Transaction(connection));
        } catch (Exception ex) {
            if (connection != null)
                connection.rollback();
            throw ex;
        } finally {
            if (connection != null) {
                try {
                    connection.setAutoCommit(true);
                } catch (SQLException ignore) {
                }
                release(connection);
            }
        }
    }
}

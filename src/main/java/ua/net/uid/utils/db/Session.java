package ua.net.uid.utils.db;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class Session {
    protected abstract Connection connection() throws SQLException;

    protected abstract void release(Connection connection);

    public Processor query(String query, Object... params) {
        return new Processor(this, query, params);
    }

    public Batch batch(String query) {
        return new Batch(this, query);
    }

    public void block(Block block) throws SQLException {
        Connection connection = null;
        try {
            connection = connection();
            block.execute(new SubSession(connection));
        } finally {
            release(connection);
        }
    }

    private static final class SubSession extends Session {
        private final Connection connection;

        SubSession(final Connection connection) {
            this.connection = connection;
        }

        @Override
        protected Connection connection() {
            return connection;
        }

        @Override
        protected void release(Connection connection) {
        }
    }
}

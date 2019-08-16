package ua.net.uid.utils.db;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public interface Connector {
    Connection get() throws SQLException;

    void release(Connection connection);

    void close();

    final class Pooled implements Connector {
        private final DataSource source;

        public Pooled(DataSource source) {
            this.source = source;
        }

        @Override
        public Connection get() throws SQLException {
            return source.getConnection();
        }

        @Override
        public void release(Connection connection) {
            if (connection != null) {
                try { connection.close(); } catch (final SQLException ignored) {}
            }
        }

        @Override
        public void close() {
        }
    }

    final class Source implements Connector {
        private final DataSource source;
        private Connection connection = null;

        public Source(DataSource source) {
            this.source = source;
        }

        @Override
        public Connection get() throws SQLException {
            if (connection == null || connection.isClosed())
                connection = source.getConnection();
            return connection;
        }

        @Override
        public void release(Connection connection) {
        }

        @Override
        public void close() {
            if (connection != null) {
                try { connection.close(); } catch (final SQLException ignored) {}
                connection = null;
            }
        }
    }

    final class Static implements Connector {
        private final Connection connection;

        public Static(Connection connection) {
            this.connection = connection;
        }

        @Override
        public Connection get() {
            return connection;
        }

        @Override
        public void release(Connection connection) {
        }

        @Override
        public void close() {
            if (connection != null) {
                try { connection.close(); } catch (final SQLException ignored) {}
            }
        }
    }
}

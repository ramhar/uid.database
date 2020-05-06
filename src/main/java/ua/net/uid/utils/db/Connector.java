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

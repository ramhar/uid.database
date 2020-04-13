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

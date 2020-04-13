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

import java.sql.*;
import java.util.*;

public final class Processor {
    private final Session session;
    private final String query;
    private final Object[] params;

    Processor(Session session, String query, Object[] params) {
        this.session = session;
        this.query = query;
        this.params = params;
    }

    static void bind(PreparedStatement statement, Object[] params) throws SQLException {
        if (params != null && params.length > 0) {
            int count = params.length;
            for (int i = 0; i < count; ) {
                final Object param = params[i++];
                if (param instanceof Enum || param instanceof CharSequence) {
                    statement.setString(i, param.toString());
                } else {
                    statement.setObject(i, param);
                }
            }
        }
    }

    public boolean execute() throws SQLException {
        return process(Connection::prepareStatement, PreparedStatement::execute);
    }

    public int update() throws SQLException {
        return process(Connection::prepareStatement, PreparedStatement::executeUpdate);
    }

    public <T> T update(final Outcome<T> outcome) throws SQLException {
        return process(
                (connection, query) -> connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS),
                statement -> {
                    ResultSet result = null;
                    try {
                        statement.execute();
                        result = statement.getGeneratedKeys();
                        return outcome.process(result);
                    } finally {
                        if (result != null) {
                            try { result.close(); } catch (final SQLException ignored) {}
                        }
                    }
                }
        );
    }

    public <T> T select(final Outcome<T> outcome) throws SQLException {
        return process(
                Connection::prepareStatement,
                statement -> {
                    ResultSet result = null;
                    try {
                        result = statement.executeQuery();
                        return outcome.process(result);
                    } finally {
                        if (result != null) {
                            try { result.close(); } catch (final SQLException ignored) {}
                        }
                    }
                }
        );
    }

    public <T> T scalar(final Fetcher<T> fetcher) throws SQLException {
        return select(result -> result.next() ? fetcher.fetch(result) : null);
    }

    public <T> void foreach(final Fetcher<T> fetcher) throws SQLException {
        select(result -> {
            while (result.next())
                fetcher.fetch(result);
            return null;
        });
    }

    public <T> void foreach(final Fetcher<T> fetcher, final Callback<T> callback) throws SQLException {
        select(result -> {
            while (result.next())
                callback.call(fetcher.fetch(result));
            return null;
        });
    }

    public <T> void collect(final Fetcher<T> fetcher, final Collection<T> collection) throws SQLException {
        select(result -> {
            while (result.next())
                collection.add(fetcher.fetch(result));
            return null;
        });
    }

    public <T> List<T> list(final Fetcher<T> fetcher) throws SQLException {
        ArrayList<T> list = new ArrayList<>();
        collect(fetcher, list);
        return list;
    }

    public <T> Set<T> set(final Fetcher<T> fetcher) throws SQLException {
        LinkedHashSet<T> set = new LinkedHashSet<>();
        collect(fetcher, set);
        return set;
    }

    @SuppressWarnings("unchecked")
    public <K, T> void map(final Fetcher<T> fetcher, final String key, final Map<K, T> map) throws SQLException {
        select(result -> {
            while (result.next())
                map.put((K) result.getObject(key), fetcher.fetch(result));
            return null;
        });
    }

    public <K, T> Map<K, T> map(final Fetcher<T> fetcher, final String key) throws SQLException {
        Map<K, T> map = new LinkedHashMap<>();
        map(fetcher, key, map);
        return map;
    }

    protected <T> T process(Preparer preparer, final Handler<T> callback) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = session.connection();
            statement = preparer.prepare(connection, query);
            bind(statement, params);
            return callback.handle(statement);
        } finally {
            if (statement != null) {
                try { statement.close(); } catch (final SQLException ignored) {}
            }
            session.release(connection);
        }
    }

    public interface Callback<P> {
        void call(P param);
    }

    private interface Handler<T> {
        T handle(final PreparedStatement statement) throws SQLException;
    }

    private interface Preparer {
        PreparedStatement prepare(final Connection connection, final String query) throws SQLException;
    }
}

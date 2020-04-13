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
package ua.net.uid.utils.db.query;

import ua.net.uid.utils.db.Processor;
import ua.net.uid.utils.db.Session;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryBuilder implements Query {
    private final StringBuilder builder = new StringBuilder();
    private final List<Object> params = new ArrayList<>();

    public QueryBuilder append(QueryPart part) {
        if (part != null) {
            part.build(builder);
            part.bind(params);
        }
        return this;
    }

    public QueryBuilder appendIf(boolean con, QueryPart part) {
        if (con && part != null) {
            part.build(builder);
            part.bind(params);
        }
        return this;
    }

    public QueryBuilder append(CharSequence prefix, QueryPart part) {
        if (part != null) {
            builder.append(prefix);
            part.build(builder);
            part.bind(params);
        }
        return this;
    }

    public QueryBuilder appendIf(boolean con, CharSequence prefix, QueryPart part) {
        if (con && part != null) {
            builder.append(prefix);
            part.build(builder);
            part.bind(params);
        }
        return this;
    }

    public QueryBuilder append(CharSequence part) {
        if (part != null)
            builder.append(part);
        return this;
    }

    public QueryBuilder appendIf(boolean con, CharSequence part) {
        if (con && part != null)
            builder.append(part);
        return this;
    }

    public QueryBuilder append(CharSequence part, Object... params) {
        if (part == null || part.length() == 0)
            throw new IllegalArgumentException("not empty part required");
        builder.append(part);
        Collections.addAll(this.params, params);
        return this;
    }

    public QueryBuilder appendIf(boolean con, CharSequence part, Object... params) {
        if (con) {
            if (part == null || part.length() == 0)
                throw new IllegalArgumentException("not empty part required");
            builder.append(part);
            Collections.addAll(this.params, params);
        }
        return this;
    }

    public QueryBuilder append(String part) {
        if (part != null)
            builder.append(part);
        return this;
    }

    public QueryBuilder appendIf(boolean con, String part) {
        if (con && part != null)
            builder.append(part);
        return this;
    }

    public QueryBuilder append(String part, Object... params) {
        if (part == null || part.length() == 0)
            throw new IllegalArgumentException("not empty part required");
        builder.append(part);
        Collections.addAll(this.params, params);
        return this;
    }

    public QueryBuilder appendIf(boolean con, String part, Object... params) {
        if (con) {
            if (part == null || part.length() == 0)
                throw new IllegalArgumentException("not empty part required");
            builder.append(part);
            Collections.addAll(this.params, params);
        }
        return this;
    }

    public Processor on(Session session) {
        return session.query(getQuery(), getParams());
    }

    @Override
    public String getQuery() {
        return builder.toString();
    }

    @Override
    public Object[] getParams() {
        return params.toArray();
    }
}

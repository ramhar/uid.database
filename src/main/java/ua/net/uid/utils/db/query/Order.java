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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Order implements QueryPart {
    private final StringBuilder expressions = new StringBuilder();
    private final ArrayList<Object> params = new ArrayList<>(1);

    private Order() {
    }

    public static Order by() {
        return new Order();
    }

    public Order asc(CharSequence expression, Object... args) {
        return append(expression, " ASC", args);
    }

    public Order desc(CharSequence expression, Object... args) {
        return append(expression, " DESC", args);
    }

    public Order ascNullFirst(CharSequence expression, Object... args) {
        return append(expression, " ASC NULLS FIRST", args);
    }

    public Order ascNullLast(CharSequence expression, Object... args) {
        return append(expression, " ASC NULLS LAST", args);
    }

    public Order descNullFirst(CharSequence expression, Object... args) {
        return append(expression, " DESC NULLS FIRST", args);
    }

    public Order descNullLast(CharSequence expression, Object... args) {
        return append(expression, " DESC NULLS LAST", args);
    }

    private Order append(CharSequence expression, String suffix, Object[] args) {
        expressions.append(expressions.length() == 0 ? " ORDER BY " : ", ").append(expression).append(suffix);
        if (args != null) Collections.addAll(params, args);
        return this;
    }

    @Override
    public void build(StringBuilder builder) {
        builder.append(expressions);
    }

    @Override
    public void bind(List<Object> params) {
        params.addAll(this.params);
    }
}

package ua.net.uid.utils.db.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public abstract class Condition implements QueryPart {
    public static Condition raw(CharSequence condition, Object... params) {
        if (condition == null)
            throw new IllegalArgumentException("condition is null");
        return new Raw(condition, params);
    }

    public static Condition not(CharSequence condition, Object... params) {
        return new Not(raw(condition, params));
    }

    public static Condition not(Condition condition) {
        if (condition == null) {
            return null;
        } else if (condition instanceof Not) {
            return ((Not) condition).condition;
        } else {
            return new Not(condition);
        }
    }

    public static Condition and(Condition left, Condition right) {
        if (left != null) {
            return right == null ? left : new And(left, right);
        } else {
            return right;
        }
    }

    public static Condition and(Condition... conditions) {
        if (conditions == null || conditions.length == 0) return null;
        return conditions.length == 1 ? conditions[0] : new And(conditions);
    }

    public static Condition or(Condition... conditions) {
        if (conditions == null || conditions.length == 0) return null;
        return conditions.length == 1 ? conditions[0] : new Or(conditions);
    }
    
    public static Condition in(CharSequence left, Collection<?> items) {
        if (left == null || left.length() == 0)
            throw new IllegalArgumentException("left side parameter for 'in' condition is null");
        if (items == null || items.isEmpty()) return null;
        return new In(left, items.toArray(new Object[items.size()]));
    }

    public static Condition in(CharSequence left, Object... items) {
        if (left == null || left.length() == 0)
            throw new IllegalArgumentException("left side parameter for 'in' condition is null");
        if (items == null || items.length == 0) return null;
        return new In(left, items);
    }

    public Object[] toParams(Object... post) {
        ArrayList<Object> params = new ArrayList<>();
        bind(params);
        if (post != null) Collections.addAll(params, post);
        return params.toArray();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        build(builder);
        return builder.toString();
    }

    static final class Raw extends Condition {
        private final CharSequence condition;
        private final Object[] params;

        Raw(CharSequence condition, Object[] params) {
            this.condition = condition;
            this.params = params;
        }

        @Override
        public void build(StringBuilder builder) {
            builder.append(condition);
        }

        @Override
        public void bind(List<Object> params) {
            Collections.addAll(params, this.params);
        }
    }

    static final class Not extends Condition {
        private final Condition condition;

        Not(Condition condition) {
            this.condition = condition;
        }

        @Override
        public void build(StringBuilder builder) {
            builder.append("NOT(");
            condition.build(builder);
            builder.append(')');
        }

        @Override
        public void bind(List<Object> params) {
            condition.bind(params);
        }
    }

    static abstract class Block extends Condition {
        final List<Condition> conditions;

        Block(Condition... conditions) {
            this.conditions = new ArrayList<>(conditions.length);
            for (Condition condition : conditions)
                if (condition != null) {
                    if (condition.getClass() == getClass()) {
                        this.conditions.addAll(((Block) condition).conditions);
                    } else {
                        this.conditions.add(condition);
                    }
                }
        }

        @Override
        public final void bind(List<Object> params) {
            for (Condition condition : conditions)
                condition.bind(params);
        }

        final void build(StringBuilder builder, String div) {
            if (conditions.size() > 1) {
                builder.append("(");
                conditions.get(0).build(builder);
                for (int i = 1; i < conditions.size(); ++i) {
                    builder.append(div);
                    conditions.get(i).build(builder);
                }
                builder.append(")");
            } else {
                conditions.get(0).build(builder);
            }
        }
    }

    static final class And extends Block {
        And(Condition... conditions) {
            super(conditions);
        }

        @Override
        public void build(StringBuilder builder) {
            build(builder, ") AND (");
        }
    }

    static final class Or extends Block {
        Or(Condition... conditions) {
            super(conditions);
        }

        @Override
        public void build(StringBuilder builder) {
            build(builder, ") OR (");
        }
    }

    static final class In extends Condition {
        private final CharSequence left;
        private final Object[] items;

        public In(CharSequence left, Object[] items) {
            this.left = left;
            this.items = items;
        }

        @Override
        public void build(StringBuilder builder) {
            builder.append(left).append(" IN (?");
            for (int i = 1; i < items.length; ++i) builder.append(",?");
            builder.append(')');
        }

        @Override
        public void bind(List<Object> params) {
            Collections.addAll(params, items);
        }
    }
}

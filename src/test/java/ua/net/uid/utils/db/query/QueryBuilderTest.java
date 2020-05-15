package ua.net.uid.utils.db.query;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class QueryBuilderTest {
    @Test
    void appendQueryPart() {
        QueryBuilder builder1 = new QueryBuilder().append((QueryPart) null);
        assertEquals("", builder1.getQuery());
        assertArrayEquals(new Object[]{}, builder1.getParams());

        QueryBuilder builder2 = new QueryBuilder().append(
                new SimpleQueryPart("field = ? AND type != ?", 1, 2)
        );
        assertEquals("field = ? AND type != ?", builder2.getQuery());
        assertArrayEquals(new Object[]{1, 2}, builder2.getParams());
    }

    @Test
    void appendQueryPartWithPrefix() {
        QueryBuilder builder1 = new QueryBuilder().append(
                " WHERE ", (QueryPart) null
        );
        assertEquals("", builder1.getQuery());
        assertArrayEquals(new Object[]{}, builder1.getParams());

        QueryBuilder builder2 = new QueryBuilder().append(
                " WHERE ",
                new SimpleQueryPart("field = ? AND type != ?", 1, 2)
        );
        assertEquals(" WHERE field = ? AND type != ?", builder2.getQuery());
        assertArrayEquals(new Object[]{1, 2}, builder2.getParams());
    }

    @Test
    void appendCharSequence() {
        QueryBuilder builder1 = new QueryBuilder().append((CharSequence) null);
        assertEquals("", builder1.getQuery());
        assertArrayEquals(new Object[]{}, builder1.getParams());

        QueryBuilder builder2 = new QueryBuilder().append((CharSequence) "");
        assertEquals("", builder2.getQuery());
        assertArrayEquals(new Object[]{}, builder2.getParams());

        QueryBuilder builder3 = new QueryBuilder().append((CharSequence) "test");
        assertEquals("test", builder3.getQuery());
        assertArrayEquals(new Object[]{}, builder3.getParams());
    }

    @Test
    void appendCharSequenceWithParamsMustThrow1() {
        assertThrows(IllegalArgumentException.class, () -> new QueryBuilder().append((CharSequence) null, 1, 2));
    }

    @Test
    void appendCharSequenceWithParamsMustThrow2() {
        assertThrows(IllegalArgumentException.class, () -> new QueryBuilder().append((CharSequence) "", 1, 2));
    }

    @Test
    void appendCharSequenceWithParams() {
        QueryBuilder builder1 = new QueryBuilder().append((CharSequence) "test", 1, 2);
        assertEquals("test", builder1.getQuery());
        assertArrayEquals(new Object[]{1, 2}, builder1.getParams());
    }

    @Test
    void appendString() {
        QueryBuilder builder1 = new QueryBuilder().append((String) null);
        assertEquals("", builder1.getQuery());
        assertArrayEquals(new Object[]{}, builder1.getParams());

        QueryBuilder builder2 = new QueryBuilder().append("");
        assertEquals("", builder2.getQuery());
        assertArrayEquals(new Object[]{}, builder2.getParams());

        QueryBuilder builder3 = new QueryBuilder().append("test");
        assertEquals("test", builder3.getQuery());
        assertArrayEquals(new Object[]{}, builder3.getParams());
    }

    @Test
    void appendStringWithParamsMustThrow1() {
        assertThrows(IllegalArgumentException.class, () -> new QueryBuilder().append(null, 1, 2));
    }

    @Test
    void appendStringWithParamsMustThrow2() {
        assertThrows(IllegalArgumentException.class, () -> new QueryBuilder().append("", 1, 2));
    }

    @Test
    void appendStringWithParams() {
        QueryBuilder builder1 = new QueryBuilder().append("test", 1, 2);
        assertEquals("test", builder1.getQuery());
        assertArrayEquals(new Object[]{1, 2}, builder1.getParams());
    }

    @Test
    void complexTest() {
        QueryBuilder builder = new QueryBuilder()
                .append("SELECT t.*")
                .append(" FROM test AS t INNER JOIN cross AS c ON (t.type = c.id AND c.mask = ?)", 1)
                .append(" WHERE ", Condition.and(
                        Condition.not("disabled"),
                        Condition.raw("BIT_AND(access, ?) = ?", 2, 3)
                ))
                .append(" HAVING ", Condition.and())
                .append(Order.by().desc("t.date"))
                .append(" LIMIT ? OFFSET ?", 4, 5);
        assertEquals(
                "SELECT t.* FROM test AS t INNER JOIN cross AS c ON (t.type = c.id AND c.mask = ?) WHERE (NOT(disabled)) AND (BIT_AND(access, ?) = ?) ORDER BY t.date DESC LIMIT ? OFFSET ?",
                builder.getQuery()
        );
        assertArrayEquals(new Object[]{1, 2, 3, 4, 5}, builder.getParams());
        //System.out.println(builder.getQuery());
    }


    static class SimpleQueryPart implements QueryPart {
        private final String part;
        private final Object[] args;

        SimpleQueryPart(String part, Object... args) {
            this.part = part;
            this.args = args;
        }

        @Override
        public void build(StringBuilder builder) {
            builder.append(part);
        }

        @Override
        public void bind(List<Object> params) {
            if (args != null && args.length > 0) {
                Collections.addAll(params, args);
            }
        }
    }
}

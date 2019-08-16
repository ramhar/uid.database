package ua.net.uid.utils.db.query;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static ua.net.uid.utils.db.query.Condition.*;

class ConditionTest {
    @Test
    void rawConditionThrowTest() {
        assertThrows(IllegalArgumentException.class, () -> raw(null));
    }

    @Test
    void rawConditionTest() {
        assertEquals("title IS NOT NULL", raw("title IS NOT NULL").toString());

        Condition condition = raw("title = ? AND type = ?", "title", 1);
        assertEquals("title = ? AND type = ?", condition.toString());
        assertArrayEquals(new Object[]{"title", 1}, condition.toParams());
    }

    @Test
    void notConditionThrowTest() {
        assertThrows(IllegalArgumentException.class, () -> not(null, 1));
    }

    @Test
    void notConditionTest() {
        assertNull(not(null));

        Condition condition1 = not("title = ? AND type = ?", "title", 1);
        assertEquals("NOT(title = ? AND type = ?)", condition1.toString());
        assertArrayEquals(new Object[]{"title", 1}, condition1.toParams());

        Condition condition2 = not(condition1);
        assertEquals("title = ? AND type = ?", condition2.toString());
        assertArrayEquals(new Object[]{"title", 1}, condition2.toParams());

        Condition condition3 = not(and(condition1));
        assertEquals("title = ? AND type = ?", condition3.toString());
        assertArrayEquals(new Object[]{"title", 1}, condition3.toParams());
    }

    @Test
    void andConditionTest() {
        assertNull(and());
        //assertNull(and(new Condition[]{}));

        Condition condition1 = and(raw("title = ? AND type = ?", "title", 1));
        assertEquals("title = ? AND type = ?", condition1.toString());
        assertArrayEquals(new Object[]{"title", 1}, condition1.toParams());

        Condition condition2 = and(raw("title = ? AND type = ? OR title IS NULL", "title", 1), null);
        assertEquals("title = ? AND type = ? OR title IS NULL", condition2.toString());
        assertArrayEquals(new Object[]{"title", 1}, condition2.toParams());

        Condition condition3 = and(not("title = ? AND type = ?", "title2", 2), condition1);
        assertEquals("(NOT(title = ? AND type = ?)) AND (title = ? AND type = ?)", condition3.toString());
        assertArrayEquals(new Object[]{"title2", 2, "title", 1}, condition3.toParams());

        Condition condition4 = and(condition3, condition1);
        assertEquals("(NOT(title = ? AND type = ?)) AND (title = ? AND type = ?) AND (title = ? AND type = ?)", condition4.toString());
        assertArrayEquals(new Object[]{"title2", 2, "title", 1, "title", 1}, condition4.toParams());
    }

    @Test
    void orConditionTest() {
        assertNull(or());
        //assertNull(or(new Condition[]{}));

        Condition condition1 = or(raw("title = ? AND type = ?", "title", 1));
        assertEquals("title = ? AND type = ?", condition1.toString());
        assertArrayEquals(new Object[]{"title", 1}, condition1.toParams());

        Condition condition2 = or(raw("title = ? AND type = ? OR title IS NULL", "title", 1), null);
        assertEquals("title = ? AND type = ? OR title IS NULL", condition2.toString());
        assertArrayEquals(new Object[]{"title", 1}, condition2.toParams());

        Condition condition3 = or(not("title = ? AND type = ?", "title2", 2), condition1);
        assertEquals("(NOT(title = ? AND type = ?)) OR (title = ? AND type = ?)", condition3.toString());
        assertArrayEquals(new Object[]{"title2", 2, "title", 1}, condition3.toParams());

        Condition condition4 = or(condition3, condition1);
        assertEquals("(NOT(title = ? AND type = ?)) OR (title = ? AND type = ?) OR (title = ? AND type = ?)", condition4.toString());
        assertArrayEquals(new Object[]{"title2", 2, "title", 1, "title", 1}, condition4.toParams());
    }

    @SuppressWarnings("SpellCheckingInspection")
    @Test
    void conditionTest() {
        //System.out.println(raw.toParams());
        Condition condition = and(
                raw("date > ?", 86400),
                not("disabled"),
                and(raw("true"), not("false")),
                null,
                not(and(
                        not(or(
                                raw("title ILIKE '%'||?||'%'", "part"),
                                raw("descr ILIKE '%'||?||'%'", "part")
                        ))
                ))
        );
        assertEquals(
                "(date > ?) AND (NOT(disabled)) AND (true) AND (NOT(false)) AND ((title ILIKE '%'||?||'%') OR (descr ILIKE '%'||?||'%'))",
                condition.toString()
        );
        assertArrayEquals(
                new Object[]{86400, "part", "part"},
                condition.toParams()
        );
    }

    @Test
    void inExpressionThrowTest() {
        assertThrows(IllegalArgumentException.class, () -> in(null));
    }

    @Test
    void inExpressionTest() {
        Condition condition1 = in("field", 1, 2, 3, 4, 5);
        assertNotNull(condition1);
        assertEquals("field IN (?,?,?,?,?)", condition1.toString());
        assertArrayEquals(
                new Object[]{1, 2, 3, 4, 5},
                condition1.toParams()
        );
    }
}
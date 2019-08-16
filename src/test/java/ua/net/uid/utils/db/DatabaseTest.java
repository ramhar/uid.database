package ua.net.uid.utils.db;

import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseTest {
    private static JdbcConnectionPool source;
    private static Database db;

    @BeforeAll
    static void beforeAll() throws Exception {
        //noinspection SpellCheckingInspection
        source = JdbcConnectionPool.create(String.format("jdbc:h2:mem:x%d", System.nanoTime()), "sa", "");
        db = new Database(source, true);
        db.query("CREATE TABLE IF NOT EXISTS \"test\" (\"id\" INT IDENTITY NOT NULL PRIMARY KEY, \"name\" VARCHAR(30), \"type\" INTEGER NOT NULL DEFAULT 0)").execute();
    }

    @AfterAll
    static void afterClass() {
        source.dispose();
    }

    @Test
    void simpleExecuteTest() throws Exception {
        assertFalse(db.query("INSERT INTO \"test\" (\"name\") VALUES (?)", "Execute").execute());
        assertTrue(db.query("SELECT * FROM \"test\" WHERE \"name\" = ?", "Execute").execute());
    }

    @Test
    void simpleUpdateTest() throws Exception {
        assertEquals(1, db.query("INSERT INTO \"test\" (\"name\") VALUES (?)", "Update").update());
        //noinspection UnnecessaryBoxing
        assertEquals(Integer.valueOf(1), db.query("SELECT COUNT(*) FROM \"test\" WHERE \"name\" = ?", "Update").select(Outcome.FIRST_INT));
    }

    @Test
    void updateWithOutcomeTest() throws Exception {
        Integer id = db.query("INSERT INTO \"test\" (\"name\") VALUES (?)", "Update With Outcome").update(Outcome.FIRST_INT);
        assertNotNull(id);
        assertEquals(id, db.query("SELECT \"id\" FROM \"test\" WHERE \"name\" = ?", "Update With Outcome").select(Outcome.FIRST_INT));
    }

    @Test
    void simpleBatchTest() throws Exception {
        assertArrayEquals(
                new int[]{1, 1, 1},
                db.batch("INSERT INTO \"test\" (\"name\", \"type\") VALUES (?,?)").values("batch", 1).values("batch", 2).values("batch", 3).execute()
        );
    }

    @SuppressWarnings("UnnecessaryBoxing")
    @Test
    void blockTest() throws Exception {
        db.block(db -> {
            Connection connection = db.connection();
            assertEquals(1, db.query("INSERT INTO \"test\" (\"name\") VALUES (?)", "Block").update());
            assertEquals(Integer.valueOf(1), db.query("SELECT COUNT(*) FROM \"test\" WHERE \"name\" = 'Block'").select(Outcome.FIRST_INT));
            assertEquals(connection, db.connection());
            assertEquals(Integer.valueOf(1), db.query("DELETE FROM \"test\" WHERE \"name\" = 'Block'").update(Outcome.UPDATES_COUNT));
            assertEquals(connection, db.connection());
        });
    }

    @SuppressWarnings("UnnecessaryBoxing")
    @Test
    void transactionManualRollbackTest() throws Exception {
        db.transaction(pt -> {
            assertEquals(1, pt.query("INSERT INTO \"test\" (\"name\") VALUES (?)", "Transaction1").update());
            assertEquals(1, pt.query("INSERT INTO \"test\" (\"name\") VALUES (?)", "Transaction1").update());
            assertEquals(1, pt.query("INSERT INTO \"test\" (\"name\") VALUES (?)", "Transaction1").update());
            assertEquals(Integer.valueOf(3), pt.query("SELECT COUNT(*) FROM \"test\" WHERE \"name\" = 'Transaction1'").select(Outcome.FIRST_INT));
            pt.rollback();
        });
        assertEquals(Integer.valueOf(0), db.query("SELECT COUNT(*) FROM \"test\" WHERE \"name\" = 'Transaction1'").select(Outcome.FIRST_INT));
    }

    @SuppressWarnings("UnnecessaryBoxing")
    @Test
    void transactionExceptionRollbackTest() throws Exception {
        assertThrows(SQLException.class, () ->
                db.transaction(pt -> {
                    assertEquals(1, pt.query("INSERT INTO \"test\" (\"name\") VALUES (?)", "Transaction2").update());
                    assertEquals(Integer.valueOf(1), pt.query("SELECT COUNT(*) FROM \"test\" WHERE \"name\" = 'Transaction2'").select(Outcome.FIRST_INT));
                    throw new SQLException("Test exception");
                })
        );
        assertEquals(Integer.valueOf(0), db.query("SELECT COUNT(*) FROM \"test\" WHERE \"name\" = 'Transaction2'").select(Outcome.FIRST_INT));
    }

    @SuppressWarnings("UnnecessaryBoxing")
    @Test
    void transactionTest() throws Exception {
        db.transaction(pt -> {
            assertEquals(1, pt.query("INSERT INTO \"test\" (\"name\") VALUES (?)", "Transaction3").update());
            assertEquals(1, pt.query("INSERT INTO \"test\" (\"name\") VALUES (?)", "Transaction3").update());
            assertEquals(Integer.valueOf(2), pt.query("SELECT COUNT(*) FROM \"test\" WHERE \"name\" = 'Transaction3'").select(Outcome.FIRST_INT));
        });

        assertEquals(Integer.valueOf(2), db.query("SELECT COUNT(*) FROM \"test\" WHERE \"name\" = 'Transaction3'").select(Outcome.FIRST_INT));
    }

    @SuppressWarnings("UnnecessaryBoxing")
    @Test
    void transactionCommitTest() throws Exception {
        db.transaction(pt -> {
            assertEquals(1, pt.query("INSERT INTO \"test\" (\"name\") VALUES (?)", "Transaction4").update());
            assertEquals(Integer.valueOf(1), pt.query("SELECT COUNT(*) FROM \"test\" WHERE \"name\" = 'Transaction4'").select(Outcome.FIRST_INT));
            pt.commit();
        });
        assertEquals(Integer.valueOf(1), db.query("SELECT COUNT(*) FROM \"test\" WHERE \"name\" = 'Transaction4'").select(Outcome.FIRST_INT));

        assertEquals(Integer.valueOf(1), db.query("DELETE FROM \"test\" WHERE \"name\" = 'Transaction4'").update(Outcome.UPDATES_COUNT));
    }
}

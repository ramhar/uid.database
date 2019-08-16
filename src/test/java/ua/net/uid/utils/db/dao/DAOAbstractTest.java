package ua.net.uid.utils.db.dao;

import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ua.net.uid.utils.db.*;
import ua.net.uid.utils.db.query.Condition;
import ua.net.uid.utils.db.query.Order;
import ua.net.uid.utils.db.query.QueryBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DAOAbstractTest {
    private static JdbcConnectionPool source;
    private static Database database;

    @BeforeAll
    static void beforeAll() throws SQLException {
        source = JdbcConnectionPool.create(String.format("jdbc:h2:mem:x%d", System.nanoTime()), "sa", "");
        database = new Database(source, true);
        database.query(
                "CREATE TABLE IF NOT EXISTS test_items (" +
                        "id INT IDENTITY NOT NULL PRIMARY KEY, " +
                        "title VARCHAR(30), " +
                        "value INTEGER NOT NULL," +
                        "disabled BOOLEAN NOT NULL," +
                        "modified TIMESTAMP NOT NULL" +
                        ")"
        ).execute();

    }

    @AfterAll
    static void afterAll() {
        source.dispose();
    }

    private static Database database() {
        return database;
    }

    @Test
    void insertsAndCheckCount() throws SQLException {
        ItemDAO dao = new ItemDAO(database());
        Item item;

        assertTrue(dao.insert(item = new Item("insertsAndCheckCount 1", 1, false)));
        assertNotNull(item.getPrimaryKey());
        assertTrue(dao.insert(item = new Item("insertsAndCheckCount 2", 1, false)));
        assertNotNull(item.getPrimaryKey());
        assertTrue(dao.insert(item = new Item("insertsAndCheckCount 3", 1, false)));
        assertNotNull(item.getPrimaryKey());

        assertTrue(dao.insert(item = new Item("insertsAndCheckCount 4", 2, false)));
        assertNotNull(item.getPrimaryKey());
        assertTrue(dao.insert(item = new Item("insertsAndCheckCount 5", 2, false)));
        assertNotNull(item.getPrimaryKey());
        assertTrue(dao.insert(item = new Item("insertsAndCheckCount 6", 3, false)));
        assertNotNull(item.getPrimaryKey());

        assertEquals(3, dao.countBy(Condition.raw("value = ?", 1)));
        assertEquals(2, dao.countBy(Condition.raw("value = ?", 2)));
        assertEquals(1, dao.countBy(Condition.raw("value = ?", 3)));
    }

    @Test
    void crudTest() throws SQLException {
        ItemDAO dao = new ItemDAO(database());
        Item item1, item2;
        assertTrue(dao.insert(item1 = new Item("insertUpdateGetAndDelete", 10, false)));
        assertNotNull(item1.getPrimaryKey());

        item2 = dao.get(item1.getPrimaryKey());
        assertNotNull(item2);
        assertEquals(item1.getPrimaryKey(), item2.getPrimaryKey());
        assertEquals(item1.getTitle(), item2.getTitle());
        assertEquals(10, item2.getValue());

        item1.setValue(11);
        item1.setDisabled(true);
        assertTrue(dao.update(item1));

        item2 = dao.get(item1.getPrimaryKey());
        assertEquals(item1.getValue(), item2.getValue());
        assertEquals(item1.isDisabled(), item2.isDisabled());

        dao.delete(item1);
        assertNull(dao.get(item1.getPrimaryKey()));
    }

    @Test
    void insertAndGetBy() throws SQLException {
        ItemDAO dao = new ItemDAO(database());
        Item item1, item2;

        assertTrue(dao.insert(item1 = new Item("insertAndGetBy", 20, false)));
        item2 = dao.getBy(Condition.and(
                Condition.raw("title = ?", "insertAndGetBy"),
                Condition.not("disabled")
        ));
        assertNotNull(item2);
        assertEquals(item1.getTitle(), item2.getTitle());
        assertEquals(item1.isDisabled(), item2.isDisabled());
    }

    @Test
    void countAllInsertAndDelete() throws SQLException {
        database().transaction((db -> {
            ItemDAO dao = new ItemDAO(database());
            Item item;
            long count = dao.countAll();
            assertTrue(dao.insert(item = new Item("countAllAndInsert", 30, false)));
            assertEquals(count + 1, dao.countAll());
            assertTrue(dao.delete(item));
            assertEquals(count, dao.countAll());
        }));
    }

    @Test
    void insertsAndFind() throws SQLException {
        database().transaction((db) -> {
            ItemDAO dao = new ItemDAO(database());
            for (int i = 0; i < 10; i++) {
                assertTrue(dao.insert(new Item("insertsAndFind 1:" + i, 40, false)));
                assertTrue(dao.insert(new Item("insertsAndFind 2:" + i, 40, true)));
                assertTrue(dao.insert(new Item("insertsAndFind 3:" + i, 40, i % 2 != 0)));
            }
            long total_count = dao.countAll();
            long local_count = dao.countBy(Condition.raw("value = 40"));
            assertEquals(30, local_count);

            List<Item> all = dao.findAll();
            assertEquals(total_count, all.size());

            List<Item> local = dao.findBy(Condition.raw("value = 40"));
            assertEquals(local_count, local.size());

            List<Item> sorded = dao.findBy(Condition.raw("value = 40"), Order.by().desc("title"));
            assertEquals(local_count, sorded.size());
            assertEquals("insertsAndFind 3:9", sorded.get(0).getTitle());
            assertEquals("insertsAndFind 1:0", sorded.get(sorded.size() - 1).getTitle());
        });
    }

    private static final class Item implements Entity<Long> {
        private Long id;
        private String title;
        private int value;
        private boolean disabled;
        private Date modified;

        Item() {
        }

        Item(Long id, String title, int value, boolean disabled, Date modified) {
            this.id = id;
            this.title = title;
            this.value = value;
            this.disabled = disabled;
            this.modified = modified;
        }

        Item(String title, int value, boolean disabled, Date modified) {
            this(null, title, value, disabled, modified);
        }

        Item(String title, int value, boolean disabled) {
            this(null, title, value, disabled, new Date());
        }

        @Override
        public Long getPrimaryKey() {
            return getId();
        }

        Long getId() {
            return id;
        }

        void setId(Long id) {
            this.id = id;
        }

        String getTitle() {
            return title;
        }

        void setTitle(String title) {
            this.title = title;
        }

        int getValue() {
            return value;
        }

        void setValue(int value) {
            this.value = value;
        }

        boolean isDisabled() {
            return disabled;
        }

        void setDisabled(boolean disabled) {
            this.disabled = disabled;
        }

        Date getModified() {
            return modified;
        }

        void setModified(Date modified) {
            this.modified = modified;
        }
    }

    private static final class ItemFetcher implements Fetcher<Item> {
        @Override
        public Item fetch(ResultSet result) throws SQLException {
            Item item = new Item();
            item.setId(result.getLong("id"));
            item.setTitle(result.getString("title"));
            item.setValue(result.getInt("value"));
            item.setDisabled(result.getBoolean("disabled"));
            item.setModified(result.getTimestamp("modified"));
            return item;
        }
    }

    private static final class ItemDAO extends DAOAbstract<Item, Long> {
        ItemDAO(Session session) {
            super(session);
        }

        @Override
        public String getTableName() {
            return "test_items";
        }

        @Override
        public Condition getPrimaryCondition(Long key) {
            return Condition.raw("id = ?", key);
        }

        @Override
        public Fetcher<Item> getFetcher() {
            return new ItemFetcher();
        }

        @Override
        public boolean insert(Item item) throws SQLException {
            Processor processor = new QueryBuilder().append("INSERT INTO ").append(getTableName()).append(
                    " (title, value, disabled, modified) VALUES (?,?,?,?)"
                    , item.getTitle(), item.getValue(), item.isDisabled(), item.getModified()
            ).on(getSession());
            if (item.getId() != null) {
                return processor.update() > 0;
            } else {
                item.setId(processor.update(Outcome.FIRST_LONG));
                return item.getId() != null;
            }
        }

        @Override
        public boolean update(Item item, Long key) throws SQLException {
            return new QueryBuilder().append("UPDATE ").append(getTableName()).append(" SET ")
                    .appendIf(!key.equals(item.getPrimaryKey()), "id = ?, ", item.getId())
                    .append(
                            "title = ?, value = ?, disabled = ?, modified = ?"
                            , item.getTitle(), item.getValue(), item.isDisabled(), item.getModified()
                    )
                    .append(" WHERE ", getPrimaryCondition(key))
                    .on(getSession()).update() > 0;
        }
    }
}
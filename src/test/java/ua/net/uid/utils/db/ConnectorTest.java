package ua.net.uid.utils.db;

import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.*;

class ConnectorTest {
    private static JdbcConnectionPool source;

    @BeforeAll
    static void beforeAll() {
        source = JdbcConnectionPool.create(String.format("jdbc:h2:mem:x%d", System.nanoTime()), "sa", "");
    }

    @AfterAll
    static void afterAll() {
        source.dispose();
    }

    @Test
    void pooledConnector() throws Exception {
        assertEquals(0, source.getActiveConnections());

        Connector connector = new Connector.Pooled(source);
        assertEquals(0, source.getActiveConnections());

        Connection connection1 = connector.get();
        assertNotNull(connection1);
        assertFalse(connection1.isClosed());
        assertEquals(1, source.getActiveConnections());

        connector.close();
        assertEquals(1, source.getActiveConnections());

        Connection connection2 = connector.get();
        assertNotNull(connection2);
        assertFalse(connection2.isClosed());
        assertEquals(2, source.getActiveConnections());
        assertNotEquals(connection1, connection2);

        connector.release(connection1);
        assertTrue(connection1.isClosed());
        assertEquals(1, source.getActiveConnections());

        connector.release(connection2);
        assertTrue(connection2.isClosed());
        assertEquals(0, source.getActiveConnections());
    }

    @Test
    void sourceConnector() throws Exception {
        assertEquals(0, source.getActiveConnections());

        Connector connector = new Connector.Source(source);
        assertEquals(0, source.getActiveConnections());

        Connection connection1 = connector.get();
        assertNotNull(connection1);
        assertFalse(connection1.isClosed());
        assertEquals(1, source.getActiveConnections());

        connector.release(connection1);
        assertFalse(connection1.isClosed());
        assertEquals(1, source.getActiveConnections());

        connector.close();
        assertTrue(connection1.isClosed());
        assertEquals(0, source.getActiveConnections());
    }

    @Test
    void staticConnector() throws Exception {
        assertEquals(0, source.getActiveConnections());

        Connector connector = new Connector.Static(source.getConnection());
        assertEquals(1, source.getActiveConnections());

        Connection connection1 = connector.get();
        assertNotNull(connection1);
        assertFalse(connection1.isClosed());
        assertEquals(1, source.getActiveConnections());

        Connection connection2 = connector.get();
        assertNotNull(connection2);
        assertFalse(connection2.isClosed());
        assertEquals(1, source.getActiveConnections());
        assertEquals(connection1, connection2);

        connector.release(connection1);
        assertFalse(connection1.isClosed());
        assertEquals(1, source.getActiveConnections());

        connector.release(connection2);
        assertFalse(connection2.isClosed());
        assertEquals(1, source.getActiveConnections());

        connector.close();
        assertTrue(connection1.isClosed());
        assertEquals(0, source.getActiveConnections());
    }

}

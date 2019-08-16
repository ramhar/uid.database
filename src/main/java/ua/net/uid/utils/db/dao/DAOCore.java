package ua.net.uid.utils.db.dao;

import ua.net.uid.utils.db.Session;

public abstract class DAOCore implements DAO {
    private final Session session;

    public DAOCore(Session session) {
        this.session = session;
    }

    @Override
    public Session getSession() {
        return session;
    }
}

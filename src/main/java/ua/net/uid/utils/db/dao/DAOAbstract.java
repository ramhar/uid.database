package ua.net.uid.utils.db.dao;

import ua.net.uid.utils.db.Session;

import java.io.Serializable;

public abstract class DAOAbstract<T extends Entity<PK>, PK extends Serializable> extends DAOCore implements DAOBase<T, PK> {
    public DAOAbstract(Session session) {
        super(session);
    }
}

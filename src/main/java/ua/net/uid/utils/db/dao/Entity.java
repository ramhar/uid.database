package ua.net.uid.utils.db.dao;

import java.io.Serializable;

public interface Entity<PK extends Serializable> {
    PK getPrimaryKey();
}

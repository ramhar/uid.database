package ua.net.uid.utils.db.query;

import java.util.List;

public interface QueryPart {
    void build(StringBuilder builder);

    void bind(List<Object> params);
}

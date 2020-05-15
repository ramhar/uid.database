/*
 * Copyright 2020 nightfall.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ua.net.uid.utils.db.dao;

import ua.net.uid.utils.db.Fetcher;
import ua.net.uid.utils.db.Outcome;
import ua.net.uid.utils.db.query.Condition;
import ua.net.uid.utils.db.query.Order;
import ua.net.uid.utils.db.query.QueryBuilder;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;
import ua.net.uid.utils.db.Processor;

public interface DAOBase<T extends Entity<PK>, PK extends Serializable> extends DAO {
    String getTableName();

    Condition getPrimaryCondition(PK key);

    default Order getDefaultOrder() {
        return null;
    }

    Fetcher<T> getFetcher();

    default T get(PK key) throws SQLException {
        return getBy(getPrimaryCondition(key));
    }

    default T getBy(Condition condition) throws SQLException {
        return new QueryBuilder()
                .append("SELECT * FROM ").append(getTableName())
                .append(" WHERE ", condition)
                .append(" LIMIT 1")
                .on(getSession()).scalar(getFetcher());
    }

    default long countAll() throws SQLException {
        return getSession().query("SELECT COUNT(*) FROM " + getTableName()).select(Outcome.FIRST_LONG);
    }

    default List<T> findAll() throws SQLException {
        return findAll(getDefaultOrder());
    }
    
    default void foreachAll(Processor.Callback<T> callback) throws SQLException {
        foreachAll(callback, getDefaultOrder());
    }

    default List<T> findAll(Order order) throws SQLException {
        return new QueryBuilder().append("SELECT * FROM ").append(getTableName())
                .append(order).on(getSession()).list(getFetcher());
    }
    
    default void foreachAll(Processor.Callback<T> callback, Order order) throws SQLException {
        new QueryBuilder()
                .append("SELECT * FROM ").append(getTableName())
                .append(order)
                .on(getSession()).foreach(getFetcher(), callback);
    }

    default List<T> findAll(long limit, long offset) throws SQLException {
        return findAll(getDefaultOrder(), limit, offset);
    }
    
    default void foreachAll(Processor.Callback<T> callback, long limit, long offset) throws SQLException {
        foreachAll(callback, getDefaultOrder(), limit, offset);
    }

    default List<T> findAll(Order order, long limit, long offset) throws SQLException {
        return new QueryBuilder().append("SELECT * FROM ").append(getTableName())
                .append(order).on(getSession()).list(getFetcher());
    }
    
    default void foreachAll(Processor.Callback<T> callback, Order order, long limit, long offset) throws SQLException {
        new QueryBuilder()
                .append("SELECT * FROM ").append(getTableName())
                .append(order)
                .append(" LIMIT ? OFFSET ?", limit, offset)
                .on(getSession()).foreach(getFetcher(), callback);
    }

    default long countBy(Condition condition) throws SQLException {
        return new QueryBuilder()
                .append("SELECT COUNT(*) FROM ")
                .append(getTableName())
                .append(" WHERE ", condition)
                .on(getSession()).select(Outcome.FIRST_LONG);
    }

    default List<T> findBy(Condition condition) throws SQLException {
        return findBy(condition, getDefaultOrder());
    }

    default void foreachBy(Processor.Callback<T> callback, Condition condition) throws SQLException {
        foreachBy(callback, condition, getDefaultOrder());
    }    

    default List<T> findBy(Condition condition, Order order) throws SQLException {
        return new QueryBuilder()
                .append("SELECT * FROM ").append(getTableName())
                .append(" WHERE ", condition)
                .append(order)
                .on(getSession()).list(getFetcher());
    }
    
    default void foreachBy(Processor.Callback<T> callback, Condition condition, Order order) throws SQLException {
        new QueryBuilder()
                .append("SELECT * FROM ").append(getTableName())
                .append(" WHERE ", condition)
                .append(order)
                .on(getSession()).foreach(getFetcher(), callback);
    }

    default List<T> findBy(Condition condition, long limit, long offset) throws SQLException {
        return findBy(condition, getDefaultOrder(), limit, offset);
    }
    
    default void foreachBy(Processor.Callback<T> callback, Condition condition, long limit, long offset) throws SQLException {
        foreachBy(callback, condition, getDefaultOrder(), limit, offset);
    }

    default List<T> findBy(Condition condition, Order order, long limit, long offset) throws SQLException {
        return new QueryBuilder()
                .append("SELECT * FROM ").append(getTableName())
                .append(" WHERE ", condition)
                .append(" LIMIT ? OFFSET ?", limit, offset)
                .append(order)
                .on(getSession()).list(getFetcher());
    }
    
    default void foreachBy(Processor.Callback<T> callback, Condition condition, Order order, long limit, long offset) throws SQLException {
        new QueryBuilder()
                .append("SELECT * FROM ").append(getTableName())
                .append(" WHERE ", condition)
                .append(" LIMIT ? OFFSET ?", limit, offset)
                .append(order)
                .on(getSession()).foreach(getFetcher(), callback);
    }

    boolean insert(T item) throws SQLException;

    boolean update(T item, PK key) throws SQLException;

    default boolean update(T item) throws SQLException {
        return update(item, item.getPrimaryKey());
    }

    default boolean delete(PK key) throws SQLException {
        return new QueryBuilder()
                .append("DELETE FROM ").append(getTableName())
                .append(" WHERE ", getPrimaryCondition(key))
                .on(getSession()).update() > 0;
    }

    default boolean delete(T item) throws SQLException {
        return delete(item.getPrimaryKey());
    }
}

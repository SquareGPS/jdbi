/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.skife.jdbi.v2.sqlobject;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Something;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.subpackage.PrivateImplementationFactory;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TestBeanBinder
{
    private DBI    dbi;
    private Handle handle;

    @Before
    public void setUp() throws Exception
    {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + UUID.randomUUID());
        dbi = new DBI(ds);
        dbi.registerMapper(new SomethingMapper());
        dbi.registerMapper(new SomeRecordMapper());
        handle = dbi.open();

        handle.execute("create table something (id int primary key, name varchar(100))");
    }

    @After
    public void tearDown() throws Exception
    {
        handle.execute("drop table something");
        handle.close();
    }

    @Test
    public void testInsert() throws Exception
    {
        Spiffy s = handle.attach(Spiffy.class);
        s.insert(new Something(2, "Bean"));

        String name = handle.createQuery("select name from something where id = 2").mapTo(String.class).first();
        assertEquals("Bean", name);
    }

    @Test
    public void testInsertRecord() throws Exception
    {
        Spiffy s = handle.attach(Spiffy.class);
        s.insert(new SomeRecord(2, "Bean"));

        String name = handle.createQuery("select name from something where id = 2").mapTo(String.class).first();
        assertEquals("Bean", name);
    }

    @Test
    public void testRead() throws Exception
    {
        Spiffy s = handle.attach(Spiffy.class);
        handle.insert("insert into something (id, name) values (17, 'Phil')");
        Something phil = s.findByEqualsOnBothFields(new Something(17, "Phil"));
        assertEquals("Phil", phil.getName());
    }

    @Test
    public void testReadRecord() throws Exception
    {
        Spiffy s = handle.attach(Spiffy.class);
        handle.insert("insert into something (id, name) values (17, 'Phil')");
        SomeRecord phil = s.findByEqualsOnBothFields(new SomeRecord(17, "Phil"));
        assertEquals("Phil", phil.name());
    }


    interface Spiffy {

        @SqlUpdate("insert into something (id, name) values (:id, :name)")
        int insert(@BindBean Something s);

        @SqlUpdate("insert into something (id, name) values (:id, :name)")
        int insert(@BindBean SomeRecord s);

        @SqlQuery("select id, name from something where id = :s.id and name = :s.name")
        Something findByEqualsOnBothFields(@BindBean("s") Something s);

        @SqlQuery("select id, name from something where id = :s.id and name = :s.name")
        SomeRecord findByEqualsOnBothFields(@BindBean("s") SomeRecord s);

        @SqlQuery("select :pi.value")
        String selectPublicInterfaceValue(@BindBean(value = "pi", type = PublicInterface.class) PublicInterface pi);
    }

    @Test
    public void testBindingPrivateTypeUsingPublicInterface() throws Exception
    {
        Spiffy s = handle.attach(Spiffy.class);
        assertEquals("IShouldBind", s.selectPublicInterfaceValue(PrivateImplementationFactory.create()));
    }

    public interface PublicInterface {
        String getValue();
    }

    public record SomeRecord(int id, String name) {

    }

    public static class SomeRecordMapper implements ResultSetMapper<SomeRecord> {
        @Override
        public SomeRecord map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            return new SomeRecord(r.getInt("id"), r.getString("name"));
        }
    }
}

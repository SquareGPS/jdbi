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

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Something;
import org.skife.jdbi.v2.exceptions.TransactionException;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.sqlobject.subpackage.SomethingDao;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class TestClassBasedSqlObject
{
    private DBI    dbi;
    private Handle handle;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() throws Exception
    {
        dbi = new DBI("jdbc:h2:mem:" + UUID.randomUUID());
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
    public void testPassThroughMethod() throws Exception
    {
        Dao dao = handle.attach(Dao.class);
        dao.insert(3, "Cora");

        Something c = dao.findByIdHeeHee(3);
        assertThat(c, equalTo(new Something(3, "Cora")));
    }

    @Test
    public void testUnimplementedMethod() throws Exception
    {
        Dao dao = handle.attach(Dao.class);

        exception.expect(AbstractMethodError.class);
        exception.expectCause(CoreMatchers.<Throwable>instanceOf(AbstractMethodError.class));

        dao.totallyBroken();
    }

    @Test
    public void testPassThroughMethodWithDaoInAnotherPackage() throws Exception
    {
        SomethingDao dao = handle.attach(SomethingDao.class);
        dao.insert(3, "Cora");

        Something c = dao.findByIdHeeHee(3);
        assertThat(c, equalTo(new Something(3, "Cora")));
    }

    @Test(expected = AbstractMethodError.class)
    public void testUnimplementedMethodWithDaoInAnotherPackage() throws Exception
    {
        SomethingDao dao = handle.attach(SomethingDao.class);
        dao.totallyBroken();
    }

    @Test
    public void testSimpleTransactionsSucceed() throws Exception
    {
        final SomethingDao dao = dbi.onDemand(SomethingDao.class);

        dao.insertInSingleTransaction(10, "Linda");
    }

    @Test
    public void testNestedTransactionsWithoutLevel()
    {
        final SomethingDao dao = dbi.onDemand(SomethingDao.class);
        dao.insertInNestedTransaction(11, "Angelina");
    }

    @Test
    public void testNestedTransactionsWithSameLevel()
    {
        final SomethingDao dao = dbi.onDemand(SomethingDao.class);
        dao.insertInNestedTransactionSerializable(11, "Angelina");
    }

    @Test(expected = TransactionException.class)
    public void testNestedTransactionsDifferentLevel()
    {
        final SomethingDao dao = dbi.onDemand(SomethingDao.class);
        dao.insertInNestedTransactionReadCommitted(11, "Angelina");
    }

    @RegisterMapper(SomethingMapper.class)
    public static abstract class Dao
    {
        @SqlUpdate("insert into something (id, name) values (:id, :name)")
        public abstract void insert(@Bind("id") int id, @Bind("name") String name);

        @SqlQuery("select id, name from something where id = :id")
        public abstract Something findById(@Bind("id") int id);

        public Something findByIdHeeHee(int id) {
            return findById(id);
        }

        public abstract void totallyBroken();

    }

}

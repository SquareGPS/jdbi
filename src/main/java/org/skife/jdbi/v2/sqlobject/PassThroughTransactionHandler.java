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

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionIsolationLevel;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.exceptions.TransactionException;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

class PassThroughTransactionHandler extends PassThroughHandler {
    private final TransactionIsolationLevel isolation;

    PassThroughTransactionHandler(Transaction tx) {
        this.isolation = tx.value();
    }

    @Override
    public Object invoke(SqlObject sqlObject, HandleDing ding, final Object target, final Object[] args, final Method mp, Callable<Object> superCall) throws Throwable {
        ding.retain("pass-through-transaction");
        try {
            Handle h = ding.getHandle();

            if (isolation == TransactionIsolationLevel.INVALID_LEVEL) {
                if (h.isInTransaction()) {
                    return super.invoke(sqlObject, ding, target, args, mp, superCall);
                } else {
                    return h.inTransaction(createCallback(superCall));
                }
            } else {
                if (h.isInTransaction()) {
                    if (h.getTransactionIsolationLevel() != isolation) {
                        throw new TransactionException(
                                "Tried to execute nested transaction with isolation level " + isolation + ", "
                                        + "but already running in a transaction with isolation level " + h.getTransactionIsolationLevel() + ".");
                    } else {
                        return super.invoke(sqlObject, ding, target, args, mp, superCall);
                    }
                } else {
                    return h.inTransaction(isolation, createCallback(superCall));
                }
            }
        } finally {
            ding.release("pass-through-transaction");
        }
    }

    private static TransactionCallback<Object> createCallback(Callable<Object> superCall) {
        return new TransactionCallback<Object>() {
            @Override
            public Object inTransaction(Handle conn, TransactionStatus status) throws Exception {
                try {
                    return superCall.call();
                } catch (Throwable throwable) {
                    if (throwable instanceof Exception) {
                        throw (Exception) throwable;
                    } else {
                        throw new RuntimeException(throwable);
                    }
                }
            }
        };
    }
}

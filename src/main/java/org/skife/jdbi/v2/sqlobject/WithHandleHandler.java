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
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

class WithHandleHandler implements Handler
{
    @Override
    public Object invoke(SqlObject sqlObject, HandleDing h, Object target, Object[] args, Method mp, Callable<Object> superCall)
    {
        final Handle handle = h.getHandle();
        final HandleCallback<?> callback = (HandleCallback<?>) args[0];
        try {
            return callback.withHandle(handle);
        }
        catch (Exception e) {
            throw new CallbackFailedException(e);
        }
    }
}

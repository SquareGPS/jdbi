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

import com.fasterxml.classmate.MemberResolver;
import com.fasterxml.classmate.ResolvedTypeWithMembers;
import com.fasterxml.classmate.TypeResolver;
import com.fasterxml.classmate.members.ResolvedMethod;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.TypeCache;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.*;
import net.bytebuddy.matcher.ElementMatchers;
import org.skife.jdbi.v2.SqlObjectContext;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.Callable;

import static java.util.Collections.synchronizedMap;

public class SqlObject {
    private static final TypeResolver typeResolver = new TypeResolver();
    private static final Map<Method, Handler> mixinHandlers = new HashMap<>();
    private static final Map<Class<?>, Map<Method, Handler>> handlersCache = synchronizedMap(new WeakHashMap<>());
    private static final Map<Class<?>, Field> sqlObjectFieldsCache = synchronizedMap(new WeakHashMap<>());
    private static final Map<Class<?>, ResolvedTypeWithMembers> resolvedTypeCache = synchronizedMap(new WeakHashMap<>());
    private static final TypeCache<Class<?>> typeCache = new TypeCache<>(TypeCache.Sort.WEAK);
    private static final String SQL_OBJECT_FIELD_NAME = "___sqlObject___";
    private static final Object monitor = new Object();

    static {
        mixinHandlers.putAll(TransactionalHelper.handlers());
        mixinHandlers.putAll(GetHandleHelper.handlers());

    }

    @SuppressWarnings("unchecked")
    static <E extends Throwable> void throwAsUnchecked(Exception exception) throws E {
        throw (E) exception;
    }

    static <T> T buildSqlObject(final Class<T> sqlObjectType, final HandleDing handle) {
        return buildSqlObject(sqlObjectType, handle, new SqlObjectPlugin[0]);
    }

    @SuppressWarnings("unchecked")
    static <T> T buildSqlObject(final Class<T> sqlObjectType, final HandleDing handle, SqlObjectPlugin... plugins) {

        final SqlObject so = new SqlObject(sqlObjectType, handle, plugins);
        try {
            ClassLoader classLoader = sqlObjectType.getClassLoader();
            Class<?> proxyClass = typeCache.findOrInsert(classLoader, sqlObjectType, () -> {
                return new ByteBuddy()
                        .subclass(sqlObjectType)
                        .implement(CloseInternalDoNotUseThisClass.class)
                        .suffix("$SqlObject$")
                        .defineField(SQL_OBJECT_FIELD_NAME, SqlObject.class, Visibility.PUBLIC)
                        .method(ElementMatchers.any())
                        .intercept(MethodDelegation.to(SqlObject.class))
                        .ignoreAlso(ElementMatchers.isOverriddenFrom(Object.class).or(ElementMatchers.isDeclaredBy(Object.class)))
                        .make()
                        .load(classLoader, ClassLoadingStrategy.Default.INJECTION)
                        .getLoaded();
            }, monitor);
            T instance = (T) proxyClass.newInstance();
            sqlObjectFieldsCache.computeIfAbsent(proxyClass, c -> {
                try {
                    return c.getField(SQL_OBJECT_FIELD_NAME);
                } catch (NoSuchFieldException e) {
                    throwAsUnchecked(e);
                    return null;
                }
            }).set(instance, so);
            return instance;
        } catch (Exception e) {
            throwAsUnchecked(e);
            return null;
        }
    }

    private static class PluginInterceptors {
        final Map<Method, List<SqlObjectPlugin.InvocationWrapper>> invocationWrappers = new HashMap<>();

        static PluginInterceptors build(Class<?> clazz, ResolvedTypeWithMembers resolvedType, SqlObjectPlugin... plugins) {
            PluginInterceptors res = new PluginInterceptors();
            for (SqlObjectPlugin plugin : plugins) {
                for (ResolvedMethod resolvedMethod : resolvedType.getMemberMethods()) {
                    Method raw_method = resolvedMethod.getRawMember();
                    res.invocationWrappers.compute(raw_method, (k, v) -> {
                        if (v == null) {
                            v = new ArrayList<>(plugins.length);
                        }
                        plugin.invocationWrapper(clazz, k).ifPresent(v::add);
                        return v;
                    });
                }

            }
            return res;
        }

        @Override
        public String toString() {
            return invocationWrappers.toString();
        }
    }

    private static Map<Method, Handler> buildHandlersFor(Class<?> sqlObjectType, ResolvedTypeWithMembers resolvedType) {
        if (handlersCache.containsKey(sqlObjectType)) {
            return handlersCache.get(sqlObjectType);
        }

        final Map<Method, Handler> handlers = new HashMap<>();
        for (final ResolvedMethod method : resolvedType.getMemberMethods()) {
            final Method raw_method = method.getRawMember();

            if (raw_method.isAnnotationPresent(SqlQuery.class)) {
                handlers.put(raw_method, new QueryHandler(sqlObjectType, method, ResultReturnThing.forType(method)));
            } else if (raw_method.isAnnotationPresent(SqlUpdate.class)) {
                handlers.put(raw_method, new UpdateHandler(sqlObjectType, method));
            } else if (raw_method.isAnnotationPresent(SqlBatch.class)) {
                handlers.put(raw_method, new BatchHandler(sqlObjectType, method));
            } else if (raw_method.isAnnotationPresent(SqlCall.class)) {
                handlers.put(raw_method, new CallHandler(sqlObjectType, method));
            } else if (raw_method.isAnnotationPresent(CreateSqlObject.class)) {
                handlers.put(raw_method, new CreateSqlObjectHandler(raw_method.getReturnType()));
            } else if (method.getName().equals("close") && raw_method.getParameterTypes().length == 0) {
                handlers.put(raw_method, new CloseHandler());
            } else if (raw_method.isAnnotationPresent(Transaction.class)) {
                handlers.put(raw_method, new PassThroughTransactionHandler(raw_method.getAnnotation(Transaction.class)));
            } else if (mixinHandlers.containsKey(raw_method)) {
                handlers.put(raw_method, mixinHandlers.get(raw_method));
            } else {
                handlers.put(raw_method, new PassThroughHandler(raw_method));
            }
        }

        // this is an implicit mixin, not an explicit one, so we need to *always* add it
        handlers.putAll(CloseInternalDoNotUseThisClass.Helper.handlers());

        handlersCache.put(sqlObjectType, handlers);

        return handlers;
    }

    @BindingPriority(9999)
    @RuntimeType
    public static Object intercept(@FieldValue(SQL_OBJECT_FIELD_NAME) Object so,
                                   @This Object proxy,
                                   @Origin Method method,
                                   @AllArguments Object[] args,
                                   @SuperCall(nullIfImpossible = true) Callable<Object> superCall,
                                   @StubValue Object stub) throws Throwable {
        return ((SqlObject) so).intercept(proxy, method, args, superCall, stub);
    }


    private final Class<?> sqlObjectType;
    private final Map<Method, Handler> handlers;
    private final HandleDing ding;
    private final SqlObjectPlugin[] plugins;
    private final PluginInterceptors pluginInterceptors;

    SqlObject(Class<?> sqlObjectType, HandleDing ding, SqlObjectPlugin... plugins) {
        this.sqlObjectType = sqlObjectType;
        this.ding = ding;
        this.plugins = plugins;
        ResolvedTypeWithMembers resolvedType = resolvedTypeCache.computeIfAbsent(sqlObjectType, (t) -> {
            final MemberResolver mr = new MemberResolver(typeResolver);
            return mr.resolve(typeResolver.resolve(sqlObjectType), null, null);
        });
        this.handlers = buildHandlersFor(sqlObjectType, resolvedType);
        this.pluginInterceptors = PluginInterceptors.build(sqlObjectType, resolvedType, plugins);
    }

    SqlObjectPlugin[] getPlugins() {
        return plugins;
    }

    public Object intercept(Object proxy, Method method, Object[] args, Callable<Object> superCall, Object stub) throws Throwable {
        List<SqlObjectPlugin.InvocationWrapper> list = pluginInterceptors.invocationWrappers.get(method);
        if (list == null || list.isEmpty()) {
            return invoke(proxy, method, args, superCall, stub);
        } else {
            SqlObjectPlugin.Invocation invocation = () -> invoke(proxy, method, args, superCall, stub);
            for (SqlObjectPlugin.InvocationWrapper invocationWrapper : list) {
                invocation = invocationWrapper.wrap(method, args, invocation);
            }
            return invocation.invoke();
        }
    }


    private Object invoke(Object proxy, Method method, Object[] args, Callable<Object> superCall, Object stub) throws Throwable {
        Object result;
        final Handler handler = handlers.get(method);

        // If there is no handler, pretend we are just an Object and don't open a connection (Issue #82)
        if (handler == null) {
            result = superCall.call();
        } else {
            Throwable doNotMask = null;
            String methodName = method.toString();
            SqlObjectContext oldContext = ding.setContext(new SqlObjectContext(sqlObjectType, method));
            try {
                ding.retain(methodName);
                result = handler.invoke(this, ding, proxy, args, method, superCall);
            } catch (Throwable e) {
                doNotMask = e;
                throw e;
            } finally {
                ding.setContext(oldContext);
                try {
                    ding.release(methodName);
                } catch (Throwable e) {
                    if (doNotMask == null) {
                        throw e;
                    }
                }
            }
        }
        if (result == null) {
            return stub;
        } else {
            return result;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(sqlObjectType + "{");
        sb.append("handlers=").append(handlers);
        sb.append(", ding=").append(ding);
        sb.append(", plugins=").append(Arrays.toString(plugins));
        sb.append(", pluginInterceptors=").append(pluginInterceptors);
        sb.append('}');
        return sb.toString();
    }

    public static void close(Object sqlObject) {
        if (!(sqlObject instanceof CloseInternalDoNotUseThisClass)) {
            throw new IllegalArgumentException(sqlObject + " is not a sql object");
        }
        CloseInternalDoNotUseThisClass closer = (CloseInternalDoNotUseThisClass) sqlObject;
        closer.___jdbi_close___();
    }

    static String getSql(SqlCall q, Method m) {
        if (SqlQuery.DEFAULT_VALUE.equals(q.value())) {
            return m.getName();
        } else {
            return q.value();
        }
    }

    static String getSql(SqlQuery q, Method m) {
        if (SqlQuery.DEFAULT_VALUE.equals(q.value())) {
            return m.getName();
        } else {
            return q.value();
        }
    }

    static String getSql(SqlUpdate q, Method m) {
        if (SqlQuery.DEFAULT_VALUE.equals(q.value())) {
            return m.getName();
        } else {
            return q.value();
        }
    }

    static String getSql(SqlBatch q, Method m) {
        if (SqlQuery.DEFAULT_VALUE.equals(q.value())) {
            return m.getName();
        } else {
            return q.value();
        }
    }
}

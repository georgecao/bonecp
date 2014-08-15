/**
 *  Copyright 2010 Wallace Wadge
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.jolbox.boneop;

import org.easymock.EasyMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.easymock.EasyMock.*;
import static org.testng.Assert.fail;


/**
 * A utility class for the test classes.
 *
 * @author wwadge
 * @version $Revision$
 */
public class CommonTestUtils {
    /**
     * Logger handle.
     */
    private static Logger logger = LoggerFactory.getLogger(CommonTestUtils.class);
    /**
     * Helper map.
     */
    public static Map<Class<?>, Object> instanceMap;
    /**
     * A dummy query for HSQLDB.
     */
    public static final String TEST_QUERY = "SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS";
    /**
     * A Sample test query for use with HSQLDB
     */
    public static String url = "jdbc:mock";
    /**
     * hsqldb username.
     */
    public static String username = "sa";
    /**
     * hsqldb password.
     */
    public static String password = "";
    /**
     * hsqldb driver.
     */
    static String driver = "com.jolbox.bonecp.MockJDBCDriver";
    /**
     * Config file handle.
     */
    private static BoneOPConfig config = new BoneOPConfig();

    static {
        instanceMap = new HashMap<Class<?>, Object>();
        instanceMap.put(int.class, 1);
        instanceMap.put(long.class, 1L);
        instanceMap.put(byte.class, (byte) 1);
        instanceMap.put(char.class, 'a');
        instanceMap.put(double.class, 1.0);
        instanceMap.put(short.class, new Short("1"));
        instanceMap.put(float.class, new Float(1.0));
        instanceMap.put(boolean.class, true);
        instanceMap.put(int[].class, new int[]{0, 1});
        instanceMap.put(String[].class, new String[]{"test", "bar"});
        instanceMap.put(String.class, "test");


    }

    /**
     * Returns a clone of config.
     *
     * @return config clone
     */
    public static BoneOPConfig getConfigClone() {
        try {
            return config.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Pretty printing.
     */
    static void logPass() {
        logTestInfo("Passing test");
    }

    /**
     * Pretty printing.
     *
     * @param text
     */
    static void logTestInfo(String text) {
        logger.debug("-----------------------------------------------------------------------------");
        logger.debug(text);
        logger.debug("-----------------------------------------------------------------------------\n");
    }


    /**
     * Helper function.
     *
     * @param threads
     * @param connections
     * @param cpds
     * @param workDelay
     * @param doPreparedStatement
     * @return time taken
     * @throws InterruptedException
     */
    public static long startThreadTest(int threads, long connections,
                                       DataSource cpds, int workDelay, boolean doPreparedStatement) throws InterruptedException {
        CountDownLatch startSignal = new CountDownLatch(1);
        CountDownLatch doneSignal = new CountDownLatch(threads);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) { // create and start threads
            pool.execute(new ThreadTester(startSignal, doneSignal, cpds, connections, workDelay, doPreparedStatement));
        }

        long start = System.currentTimeMillis();
        startSignal.countDown(); // START TEST!
        doneSignal.await();
        long end = (System.currentTimeMillis() - start);

        pool.shutdown();
        return end;
    }


    /**
     * Create mock expectations of the given classes then invoke the given method twice (once normal + once faking an SQL exception).
     *
     * @param mockConnection
     * @param mockClass
     * @param testClass
     * @param method
     * @param args
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    private static void doTestStatementBounceMethod(ObjectHandle mockConnection, Object mockClass, Object testClass, Method method, Object... args) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        if (method.getReturnType() == void.class) {
            method.invoke(mockClass, args);

            expectLastCall().once().andThrow(new SQLException()).once();
        } else if (method.getReturnType() == int.class) {
            expect(method.invoke(mockClass, args)).andReturn(0).once().andThrow(new SQLException()).once();
        } else if (method.getReturnType() == boolean.class) {
            expect(method.invoke(mockClass, args)).andReturn(false).once().andThrow(new SQLException()).once();
        } else if (method.getReturnType() == byte.class || method.getReturnType() == short.class || method.getReturnType() == long.class || method.getReturnType() == float.class || method.getReturnType() == double.class) {
            expect(method.invoke(mockClass, args)).andReturn(0).once().andThrow(new SQLException()).once();
        } else {
            expect(method.invoke(mockClass, args)).andReturn(null).once().andThrow(new SQLException()).once();
        }


        expect(mockConnection.markPossiblyBroken((PoolException) EasyMock.anyObject())).andReturn(new PoolException()).anyTimes();
        replay(mockClass);
        if (!mockClass.equals(mockConnection)) {
            replay(mockConnection);
        }
        method.invoke(testClass, args);
        try {
            method.invoke(testClass, args); // and repeat the test with the fake exception trigger
            fail("Should have thrown an exception");
        } catch (Throwable t) {
            // do nothing
        }

        verify(mockConnection);
        if (!mockClass.equals(mockConnection)) {
            verify(mockClass);
        }
        reset(mockClass, mockConnection);
    }

    /**
     * Create mock expectations of the given classes then invoke the given method twice (once normal + once faking an SQL exception).
     *
     * @param mockConnection
     * @param testClass
     * @param skipTests
     * @param mockClass
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    public static void testStatementBounceMethod(ObjectHandle mockConnection, Object testClass, Set<String> skipTests, Object mockClass) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {

        Method[] methods = testClass.getClass().getDeclaredMethods();
        for (Method method : methods) {
            if (skipTests.contains(method.getName()) || method.getExceptionTypes().length == 0 || !method.getExceptionTypes()[0].equals(SQLException.class)) {
                continue;
            }
            Class<?>[] params = method.getParameterTypes();
            Object[] mockParams = new Object[params.length];
            for (int i = 0; i < params.length; i++) {
                mockParams[i] = CommonTestUtils.instanceMap.get(params[i]);
            }

            doTestStatementBounceMethod(mockConnection, mockClass, testClass, method, mockParams);
        }
    }


}

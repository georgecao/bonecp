/**
 * Copyright 2010 Wallace Wadge
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jolbox.boneop.listener;

import java.sql.Statement;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.boneop.ObjectHandle;
import com.jolbox.boneop.PoolException;
import com.jolbox.boneop.PoolUtil;

/**
 * A no-op implementation of the ConnectionHook interface.
 *
 * @author wallacew
 *
 */
public abstract class AbstractObjectListener implements ObjectListener {

    /**
     * Class logger.
     */
    private static final Logger logger = LoggerFactory.getLogger(AbstractObjectListener.class);

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.hooks.ConnectionHook#onAcquire(com.jolbox.bonecp.ConnectionHandle)
     */
//	@Override
    public void onAcquire(ObjectHandle connection) {
        // do nothing
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.hooks.ConnectionHook#onCheckIn(com.jolbox.bonecp.ConnectionHandle)
     */
    // @Override
    public void onCheckIn(ObjectHandle connection) {
        // do nothing
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.hooks.ConnectionHook#onCheckOut(com.jolbox.bonecp.ConnectionHandle)
     */
    // @Override
    public void onCheckOut(ObjectHandle connection) {
        // do nothing
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.hooks.ConnectionHook#onDestroy(com.jolbox.bonecp.ConnectionHandle)
     */
    // @Override
    public void onDestroy(ObjectHandle connection) {
        // do nothing
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.hooks.ConnectionHook#onAcquireFail(Exception)
     */
    // @Override
    public boolean onAcquireFail(Throwable t, AcquireFailConfig acquireConfig) {
        boolean tryAgain = false;
        String log = acquireConfig.getLogMessage();
        logger.error(log + " Sleeping for " + acquireConfig.getAcquireRetryDelayInMillis() + "ms and trying again. Attempts left: " + acquireConfig.getAcquireRetryAttempts() + ". Exception: " + t.getCause());

        try {
            Thread.sleep(acquireConfig.getAcquireRetryDelayInMillis());
            if (acquireConfig.getAcquireRetryAttempts().get() > 0) {
                tryAgain = (acquireConfig.getAcquireRetryAttempts().decrementAndGet()) > 0;
            }
        } catch (Exception e) {
            tryAgain = false;
        }

        return tryAgain;
    }

    public void onObjectOccupyTimeLimitExceeded(ObjectHandle handle, Statement statement, String sql, Map<Object, Object> logParams, long timeElapsedInNs) {
        onQueryExecuteTimeLimitExceeded(handle, statement, sql, logParams);
    }

    /**
     * @param handle
     * @param statement
     * @param sql
     * @param logParams
     */
    @Deprecated
    public void onQueryExecuteTimeLimitExceeded(ObjectHandle handle, Statement statement, String sql, Map<Object, Object> logParams) {
        onQueryExecuteTimeLimitExceeded(sql, logParams);
    }

    @Deprecated
    public void onQueryExecuteTimeLimitExceeded(String sql, Map<Object, Object> logParams) {
        StringBuilder sb = new StringBuilder("Query execute time limit exceeded. Query: ");
        sb.append(PoolUtil.fillLogParams(sql, logParams));
        logger.warn(sb.toString());
    }

    public boolean onConnectionException(ObjectHandle objectHandle, String state, Throwable t) {
        return true; // keep the default behaviour
    }

    @Override
    public ObjectState onMarkPossiblyBroken(ObjectHandle objectHandle, String state, PoolException e) {
        return ObjectState.NOP;
    }
}

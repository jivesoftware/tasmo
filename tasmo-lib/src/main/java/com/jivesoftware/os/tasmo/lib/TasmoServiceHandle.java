package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.base.service.ServiceHandle;

/**
 *
 * @author jonathan.colt
 * @param <S> Service
 */
public interface TasmoServiceHandle<S> extends ServiceHandle {

    S getService();
}

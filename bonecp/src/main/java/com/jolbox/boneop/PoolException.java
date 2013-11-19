/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jolbox.boneop;

/**
 *
 * @author george
 */
public class PoolException extends Exception {

    public PoolException(String message) {
        super(message);
    }

    public PoolException(String message, String code) {
        super(message);
    }

    public PoolException(Throwable cause) {
        super(cause);
    }
}

package io.jiouring.file;

import io.netty.channel.unix.Errors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

public class SyscallFuture {

    private static final Logger logger = LogManager.getLogger(SyscallFuture.class);

    private volatile boolean done = false;
    private volatile Thread waiter = null;
    private volatile BiConsumer<Integer, Throwable> completionHandler = null;

    private byte op = 0;
    private int result = 0;
    private Throwable exception = null;

    // NOTE: The order of the resets matters
    void reset(byte op) {
        this.op = op;
        this.result = 0;
        this.exception = null;
        this.waiter = null;
        this.completionHandler = null;
        this.done = false;
    }

    public void onComplete(BiConsumer<Integer, Throwable> handler) {
        if (this.done) {
            handler.accept(this.result, this.exception);
        } else {
            if (this.completionHandler != null) {
                throw new IllegalStateException("SyscallFuture can only register one completionHandler");
            }
            this.completionHandler = handler;
        }
    }

    void complete(int res) {
        if (this.done) {
            throw new IllegalStateException("Future is already completed");
        }

        this.result = res;
        if (res < 0) {
            this.exception = Errors.newIOException("op: " + op, res);
        }
        finish();
    }

    void fail(Throwable cause) {
        if (this.done) {
            throw new IllegalStateException("Future is already completed");
        }

        this.exception = cause;
        this.result = -1;
        finish();
    }

    private void finish() {
        this.done = true;
        try {
            if (this.completionHandler != null) {
                this.completionHandler.accept(this.result, this.exception);
            }
        } catch (Throwable t) {
            logger.fatal("User callback crashed due to an exception; this should never happen", t);
        }

        Thread w = this.waiter;
        if (w != null) LockSupport.unpark(w);
    }

    public int join() throws IOException {
        if (this.done) return report();

        this.waiter = Thread.currentThread();
        while (!this.done) LockSupport.park(this);
        return report();
    }

    private int report() throws IOException {
        if (this.exception != null) {
            if (this.exception instanceof IOException ioException) {
                throw ioException;
            }

            if (this.exception instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }

            // Wrap unknown throwables
            throw new IOException(this.exception);
        }

        return this.result;
    }
}
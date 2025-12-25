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

    /**
     * Resets the future for reuse in a pool.
     * NOTE: This is not thread-safe and must be called by the owning thread
     * only when the future is known to be free.
     */
    void reset(byte op) {
        this.op = op;
        this.result = 0;
        this.exception = null;
        this.waiter = null;
        this.completionHandler = null;
        this.done = false;
    }

    public void onComplete(BiConsumer<Integer, Throwable> handler) {
        boolean runImmediately = false;

        synchronized (this) {
            if (this.done) {
                runImmediately = true;
            } else {
                if (this.completionHandler != null) {
                    throw new IllegalStateException("SyscallFuture can only register one completionHandler");
                }
                this.completionHandler = handler;
            }
        }

        if (runImmediately) {
            safeInvokeHandler(handler, this.result, this.exception);
        }
    }

     void complete(int res) {
        BiConsumer<Integer, Throwable> handlerToRun;
        Thread waiterToUnpark;
        Throwable computedException = null;

        if (res < 0) {
            try {
                computedException = Errors.newIOException("op: " + op, res);
            } catch (Throwable t) {
                computedException = new IOException(
                    "Operation failed with res: " + res
                        + " (and failed to construct detailed error: "
                        + t.getMessage() + ")"
                );
            }
        }

        synchronized (this) {
            if (this.done) {
                throw new IllegalStateException("Future is already completed");
            }
            this.done = true;
            this.result = res;
            this.exception = computedException;

            handlerToRun = this.completionHandler;
            waiterToUnpark = this.waiter;
        }

        if (handlerToRun != null) {
            safeInvokeHandler(handlerToRun, this.result, this.exception);
        }

        if (waiterToUnpark != null) {
            LockSupport.unpark(waiterToUnpark);
        }
    }

    void fail(Throwable cause) {
        BiConsumer<Integer, Throwable> handlerToRun;
        Thread waiterToUnpark;

        synchronized (this) {
            if (this.done) {
                throw new IllegalStateException("Future is already completed");
            }
            this.done = true;
            this.result = -1;
            this.exception = cause;

            handlerToRun = this.completionHandler;
            waiterToUnpark = this.waiter;
        }

        if (handlerToRun != null) {
            safeInvokeHandler(handlerToRun, this.result, this.exception);
        }

        if (waiterToUnpark != null) {
            LockSupport.unpark(waiterToUnpark);
        }
    }

    public int join() throws IOException {
        if (this.done) return report();

        this.waiter = Thread.currentThread();

        while (!this.done) {
            LockSupport.park(this);
        }

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
            throw new IOException(this.exception);
        }
        return this.result;
    }

    private static void safeInvokeHandler(BiConsumer<Integer, Throwable> handler, int res, Throwable ex) {
        try {
            handler.accept(res, ex);
        } catch (Throwable t) {
            logger.error("User callback crashed", t);
        }
    }

    static SyscallFuture completed(int res) {
        SyscallFuture f = new SyscallFuture();
        f.complete(res);
        return f;
    }

    static SyscallFuture completed() {
        return completed(0);
    }

    static SyscallFuture failed(Throwable cause) {
        SyscallFuture f = new SyscallFuture();
        f.fail(cause);
        return f;
    }
}
package io.jiouring.file;

import io.netty.channel.unix.Errors;
import it.unimi.dsi.fastutil.ints.IntObjectBiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

public class SyscallFuture {

    private static final Logger logger = LoggerFactory.getLogger(SyscallFuture.class);

    private volatile boolean done = false;
    private volatile Thread waiter = null;
    private volatile IntThrowableBiConsumer completionHandler = null;

    private final byte op;
    private int result = 0;
    private Throwable exception = null;

    SyscallFuture(byte op) {
        this.op = op;
    }

    public void onComplete(IntThrowableBiConsumer handler) {
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
        IntThrowableBiConsumer handlerToRun;
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
        IntThrowableBiConsumer handlerToRun;
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

    private static void safeInvokeHandler(IntThrowableBiConsumer handler, int res, Throwable ex) {
        try {
            handler.accept(res, ex);
        } catch (Throwable t) {
            logger.error("User callback crashed", t);
        }
    }

    static SyscallFuture completed(byte op, int res) {
        SyscallFuture f = new SyscallFuture(op);
        f.complete(res);
        return f;
    }

    static SyscallFuture failed(byte op, Throwable cause) {
        SyscallFuture f = new SyscallFuture(op);
        f.fail(cause);
        return f;
    }

    public interface IntThrowableBiConsumer extends IntObjectBiConsumer<Throwable> {}
}
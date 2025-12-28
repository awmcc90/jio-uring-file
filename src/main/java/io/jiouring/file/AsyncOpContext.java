package io.jiouring.file;

import io.netty.channel.unix.Errors;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;

import java.io.IOException;

public class AsyncOpContext extends DefaultPromise<Integer> {

    final byte op;
    final short id;
    final int generation;
    long uringId = -1;

    AsyncOpContext(EventExecutor executor, byte op, short id, int generation) {
        super(executor);
        this.op = op;
        this.id = id;
        this.generation = generation;
        super.setUncancellable();
    }

    // Can only be used for proxying. Should never be created in the registry.
    AsyncOpContext(EventExecutor executor, byte op) {
        this(executor, op, (short) 0, -1);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public Promise<Integer> setSuccess(Integer result) {
        if (result < 0) {
            return setFailure(resolveException(result));
        }
        return super.setSuccess(result);
    }

    @Override
    public boolean trySuccess(Integer result) {
        if (result < 0) {
            return tryFailure(resolveException(result));
        }
        return super.trySuccess(result);
    }

    private Throwable resolveException(Integer res) {
        if (res >= 0) {
            throw new IllegalArgumentException("Result is non-negative: " + res);
        }

        try {
            return Errors.newIOException("op: " + op, res);
        } catch (Throwable t) {
            return new IOException(
                "Operation failed with res: " + res
                    + " (and failed to construct detailed error: "
                    + t.getMessage() + ")"
            );
        }
    }
}

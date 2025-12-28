package io.jiouring.file;

import io.netty.channel.unix.Errors;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;

import java.io.IOException;

public class NettySyscallFuture extends DefaultPromise<Integer> {

    private final byte op;

    public NettySyscallFuture(EventExecutor executor, byte op) {
        super(executor);
        this.op = op;
        super.setUncancellable();
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
            throw new IllegalArgumentException("Result is not negative: " + res);
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

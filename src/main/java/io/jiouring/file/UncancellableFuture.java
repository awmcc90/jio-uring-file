package io.jiouring.file;

import java.util.concurrent.CompletableFuture;

public class UncancellableFuture<T> extends CompletableFuture<T> {
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public void obtrudeValue(T value) {
        throw new UnsupportedOperationException("Cannot obtrude value in IoUring future");
    }

    @Override
    public void obtrudeException(Throwable throwable) {
        throw new UnsupportedOperationException("Cannot obtrude exception in IoUring future");
    }
}

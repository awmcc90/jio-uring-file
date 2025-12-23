package io.jiouring.file;

@FunctionalInterface
public interface AsyncBatch {

    /**
     * Blocks the calling thread until all operations that were
     * active in the registry are complete.
     */
    void await();
}

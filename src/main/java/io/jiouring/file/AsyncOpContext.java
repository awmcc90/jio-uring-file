package io.jiouring.file;

public class AsyncOpContext {

    public final short id;

    public volatile boolean inUse = false;
    public volatile long uringId = -1;

    public byte op = 0;
    public long startTime = 0L;

    public final SyscallFuture future = new SyscallFuture();

    public AsyncOpContext(short id) {
        this.id = id;
    }

    // NOTE: The order of the resets matters
    public void reset(byte op) {
        this.op = op;
        this.future.reset(op);
        this.startTime = System.nanoTime();
        this.uringId = -1;
        this.inUse = true;
    }
}
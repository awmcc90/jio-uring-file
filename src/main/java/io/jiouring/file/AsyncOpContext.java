package io.jiouring.file;

class AsyncOpContext {
    final short id;

    volatile boolean inUse = false;
    volatile long uringId = -1;

    byte op = 0;
    long startTime = 0L;

    final SyscallFuture future = new SyscallFuture();

    AsyncOpContext(short id) {
        this.id = id;
    }

    AsyncOpContext(byte op, short id) {
        this(id);
        reset(op);
    }

    // NOTE: The order of the resets matters
    void reset(byte op) {
        this.op = op;
        this.future.reset(op);
        this.startTime = System.nanoTime();
        this.uringId = -1;
        this.inUse = true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("AsyncOpContext{");
        sb.append("id=").append(id);
        sb.append(", op=").append(op);
        sb.append(", inUse=").append(inUse);
        if (inUse) sb.append(", uringId=").append(uringId);
        sb.append('}');
        return sb.toString();
    }
}
package io.jiouring.file;

import io.netty.util.concurrent.EventExecutor;

class AsyncOpContext {
    final byte op;
    final short id;
    final long startTime;

    final NettySyscallFuture future;

    volatile boolean inUse = true;
    volatile long uringId = -1;

    AsyncOpContext(EventExecutor executor, byte op, short id) {
        this.op = op;
        this.id = id;
        this.startTime = System.nanoTime();
        this.future = new NettySyscallFuture(executor, op);
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
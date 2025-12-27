package example.vertx;

import io.jiouring.file.IoUringFileIoHandle;
import io.jiouring.file.NativeConstants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.AsyncFileLock;
import io.vertx.core.file.FileSystemException;
import io.vertx.core.file.impl.AsyncFileLockImpl;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.PromiseInternal;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.internal.buffer.BufferInternal;
import io.vertx.core.streams.impl.InboundBuffer;
import io.vertx.core.VertxException;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileLock;

public class IoUringAsyncFileImpl implements AsyncFile {

    private final VertxInternal vertx;
    private final ContextInternal context;
    private final IoUringFileIoHandle handle;

    private final InboundBuffer<Buffer> queue;
    private Handler<Buffer> handler;
    private Handler<Throwable> exceptionHandler;
    private Handler<Void> endHandler;
    private long readPos;
    private long readLength = Long.MAX_VALUE;
    private int readBufferSize = 8192;
    private boolean readInProgress = false;

    private Handler<Void> drainHandler;
    private long writePos;
    private int maxWrites = 128 * 1024;
    private int writesOutstandingBytes = 0;

    private boolean closed;

    private final Future<AsynchronousFileChannel> lockChannel;

    public IoUringAsyncFileImpl(VertxInternal vertx, ContextInternal context, IoUringFileIoHandle handle) {
        this.vertx = vertx;
        this.context = context;
        this.handle = handle;

        this.queue = new InboundBuffer<>(context, 0);
        queue.handler(buff -> {
            if (buff.length() > 0) {
                handleBuffer(buff);
            } else {
                handleEnd();
            }
        });
        this.queue.drainHandler(v -> doRead());

        // Don't want to do this but don't have an alternative...
        this.lockChannel = vertx.executeBlocking(() -> {
            try {
                return AsynchronousFileChannel.open(
                    handle.path,
                    java.nio.file.StandardOpenOption.READ,
                    java.nio.file.StandardOpenOption.WRITE
                );
            } catch (IOException e) {
                throw new FileSystemException(e);
            }
        });
    }

    private void checkContext() {
        if (!context.inThread()) {
            throw new IllegalStateException("AsyncFile must only be used in the context that created it.");
        }
    }

    private void checkClosed() {
        if (closed) {
            throw new IllegalStateException("File handle is closed");
        }
    }

    @Override
    public AsyncFile handler(Handler<Buffer> handler) {
        checkContext();
        checkClosed();
        this.handler = handler;
        if (handler != null) {
            doRead();
        } else {
            queue.clear();
        }
        return this;
    }

    @Override
    public AsyncFile pause() {
        checkContext();
        queue.pause();
        return this;
    }

    @Override
    public AsyncFile resume() {
        checkContext();
        if (!closed) {
            queue.resume();
        }
        return this;
    }

    @Override
    public AsyncFile fetch(long amount) {
        checkContext();
        queue.fetch(amount);
        return this;
    }

    @Override
    public AsyncFile endHandler(Handler<Void> endHandler) {
        checkContext();
        this.endHandler = endHandler;
        return this;
    }

    private void doRead() {
        if (readInProgress || closed || queue.isPaused()) {
            return;
        }

        int toRead = (int) Math.min(Integer.MAX_VALUE, Math.min(readBufferSize, readLength));
        if (toRead <= 0) {
            return;
        }

        readInProgress = true;

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(toRead);

        handle
            .readAsync(buf, readPos)
            .onComplete((res, err) -> {
                readInProgress = false;

                if (err != null) {
                    buf.release();
                    handleException(err);
                    return;
                }

                assert res >= 0;

                buf.writerIndex(res);
                readPos += res;
                readLength -= res;

                if (queue.write(BufferInternal.buffer(buf))) {
                    doRead();
                }
            });
    }

    @Override
    public Future<Void> end() {
        return close();
    }

    private void handleBuffer(Buffer buff) {
        if (handler != null) {
            checkContext();
            handler.handle(buff);
        }
    }

    private void handleEnd() {
        handler = null;
        if (endHandler != null) {
            checkContext();
            endHandler.handle(null);
        }
    }

    // ==========================================================
    // WriteStream Implementation
    // ==========================================================

    @Override
    public Future<Void> write(Buffer buffer) {
        Promise<Void> p = context.promise();
        int length = buffer.length();
        doWrite(buffer, writePos, p).onSuccess(v -> writePos += length);
        return p.future();
    }

    @Override
    public Future<Void> write(Buffer buffer, long position) {
        Promise<Void> p = context.promise();
        doWrite(buffer, position, p);
        return p.future();
    }

    private Future<Void> doWrite(Buffer buffer, long position, Promise<Void> promise) {
        checkContext();
        checkClosed();

        ByteBuf buf = ((BufferInternal) buffer).getByteBuf();
        int length = buf.readableBytes();

        writesOutstandingBytes += length;
        boolean wasFull = writeQueueFull();

        handle
            .writeAsync(buf, position, false)
            .onComplete((res, err) -> {
                writesOutstandingBytes -= length;

                if (err != null) {
                    promise.fail(err);
                } else {
                    if (wasFull && !writeQueueFull() && drainHandler != null) {
                        drainHandler.handle(null);
                    }
                    promise.complete();
                }
            });

        return promise.future();
    }

    @Override
    public AsyncFile setWriteQueueMaxSize(int maxSize) {
        checkContext();
        this.maxWrites = maxSize;
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        checkContext();
        return writesOutstandingBytes >= maxWrites || !handle.canWrite();
    }

    @Override
    public AsyncFile drainHandler(Handler<Void> handler) {
        checkContext();
        this.drainHandler = handler;
        return this;
    }

    @Override
    public AsyncFile exceptionHandler(Handler<Throwable> handler) {
        checkContext();
        this.exceptionHandler = handler;
        return this;
    }

    private void handleException(Throwable t) {
        if (exceptionHandler != null) {
            exceptionHandler.handle(t);
        } else {
            context.reportException(t);
        }
    }

    @Override
    public Future<Buffer> read(Buffer buffer, int offset, long position, int length) {
        checkContext();
        checkClosed();

        Promise<Buffer> p = context.promise();
        ByteBuf slice = getByteBuf((BufferInternal) buffer, offset, length);

        handle
            .readAsync(slice, position)
            .onComplete((bytesRead, err) -> {
                if (err != null) {
                    p.fail(err);
                    return;
                }

                // Update the original buffer's writer index if necessary,
                // or assume the user manages the "valid" part of the buffer manually.
                // Vert.x contract usually implies the buffer is filled.
                // We set the writer index of the slice, which modifies underlying memory.
                // But we must update the main buffer writerIndex if we appended?
                // The API says "written into specified Buffer at position offset".
                // It does not explicitly say it appends.

                p.complete(buffer);
            });

        return p.future();
    }

    private static ByteBuf getByteBuf(BufferInternal buffer, int offset, int length) {
        ByteBuf buf = buffer.getByteBuf();
        ByteBuf slice = buf.slice(offset, length);
        slice.writerIndex(0);
        return slice;
    }

    @Override
    public Future<Void> flush() {
        checkContext();
        checkClosed();

        Promise<Void> p = context.promise();

        handle
            .fsyncAsync(false, 0, 0)
            .onComplete((res, err) -> {
                if (err != null) p.fail(err);
                else p.complete();
            });

        return p.future();
    }

    // ==========================================================
    // Getters / Setters
    // ==========================================================

    @Override
    public AsyncFile setReadPos(long readPos) {
        checkContext();
        this.readPos = readPos;
        return this;
    }

    @Override
    public AsyncFile setReadLength(long readLength) {
        checkContext();
        this.readLength = readLength;
        return this;
    }

    @Override
    public long getReadLength() {
        return readLength;
    }

    @Override
    public AsyncFile setWritePos(long writePos) {
        checkContext();
        this.writePos = writePos;
        return this;
    }

    @Override
    public long getWritePos() {
        return writePos;
    }

    @Override
    public AsyncFile setReadBufferSize(int readBufferSize) {
        checkContext();
        this.readBufferSize = readBufferSize;
        return this;
    }

    @Override
    public long sizeBlocking() {
        try {
            return size().await();
        } catch (Exception e) {
            throw new VertxException(e);
        }
    }

    @Override
    public Future<Long> size() {
        checkContext();
        Promise<Long> p = context.promise();
        ByteBuf statBuffer = Unpooled.directBuffer(256);
        handle
            .statxAsync(NativeConstants.StatxMask.SIZE, 0, statBuffer)
            .onComplete((res, err) -> {
                if (err != null) {
                    statBuffer.release();
                    p.fail(err);
                } else {
                    long size = statBuffer.getLong(0x28);
                    statBuffer.release();
                    p.complete(size);
                }
            });
        return p.future();
    }

    @Override
    public Future<AsyncFileLock> lock(long position, long size, boolean shared) {
        PromiseInternal<AsyncFileLock> promise = vertx.promise();
        lockChannel.onComplete(lcAr -> {
            if (lcAr.failed()) {
                promise.fail(new FileSystemException(lcAr.cause()));
            } else {
                AsynchronousFileChannel ch = lcAr.result();
                vertx
                    .executeBlockingInternal(() -> {
                        ch.lock(position, size, shared, promise, new CompletionHandler<>() {
                            @Override
                            public void completed(FileLock result, PromiseInternal<AsyncFileLock> p) {
                                p.complete(new AsyncFileLockImpl(p.context().owner(), result));
                            }

                            @Override
                            public void failed(Throwable t, PromiseInternal<AsyncFileLock> p) {
                                p.fail(new FileSystemException(t));
                            }
                        });
                        return null;
                    })
                    .onFailure(err -> promise.fail(new FileSystemException(err)));
            }
        });
        return promise.future();
    }

    @Override
    public AsyncFileLock tryLock(long position, long size, boolean shared) {
        checkContext();
        try {
            FileLock lock = lockChannel.await().tryLock(position, size, shared);
            return new AsyncFileLockImpl(vertx, lock);
        } catch (IOException e) {
            throw new FileSystemException(e);
        }
    }

    @Override
    public Future<Void> close() {
        if (closed) {
            return Future.succeededFuture();
        }
        closed = true;

        if (queue != null) {
            queue.clear();
        }

        Promise<Void> closeHandlePromise = context.promise();
        handle
            .closeAsync()
            .whenComplete((res, err) -> context.runOnContext(v -> {
                if (err != null) closeHandlePromise.fail(err);
                else closeHandlePromise.complete();
            }));

        Promise<Void> closeChannelPromise = context.promise();
        lockChannel
            .onComplete(ar -> {
                if (ar.failed()) closeChannelPromise.fail(ar.cause());
                else {
                    context
                        .executeBlockingInternal(() -> {
                            ar.result().close();
                            return null;
                        })
                        .onComplete(ar2 -> {
                            if (ar2.failed()) closeChannelPromise.fail(ar2.cause());
                            else closeChannelPromise.complete();
                        });
                }
            });

        return Future.all(closeHandlePromise.future(), closeChannelPromise.future())
            .mapEmpty();
    }
}
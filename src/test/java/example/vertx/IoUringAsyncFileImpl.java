package example.vertx;

import io.jiouring.file.Buffers;
import io.jiouring.file.IoUringFileIoHandle;
import io.jiouring.file.NativeConstants;
import io.netty.buffer.ByteBuf;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
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

        // Don't want to do this but don't know of an alternative...
        this.lockChannel = vertx.executeBlocking(() -> {
            try {
                return AsynchronousFileChannel.open(
                    handle.path,
                    java.nio.file.StandardOpenOption.READ,
                    java.nio.file.StandardOpenOption.WRITE
                );
            } catch (IOException e) {
                // For anonymous temp files we can't do this, so I guess return null
                // throw new FileSystemException(e);
                return null;
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
            // We are done - no need to submit just get an empty buf returned
            if (readLength == 0) {
                handleEnd();
            }
            return;
        }

        readInProgress = true;

        ByteBuf bb = Buffers.direct(toRead);
        io.netty.util.concurrent.Future<Integer> readFuture;

        try {
            readFuture = handle.readAsync(bb, readPos);
        } catch (Throwable t) {
            bb.release();
            readInProgress = false;
            handleException(t);
            return;
        }

        readFuture
            .addListener((f) -> {
                readInProgress = false;

                if (!f.isSuccess()) {
                    bb.release();
                    handleException(f.cause());
                    return;
                }

                int res = (int) f.getNow();

                if (res == 0) {
                    bb.release();
                    handleEnd();
                    return;
                }

                bb.writerIndex(bb.writerIndex() + res);
                readPos += res;
                readLength -= res;

                // Transfers ownership of bb and releases
                BufferInternal buf = BufferInternal.safeBuffer(bb);

                if (queue.write(buf) && readLength > 0) {
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

    @Override
    public Future<Void> write(Buffer buffer) {
        Promise<Void> p = context.promise();
        int length = buffer.length();
        doWrite(buffer, writePos, p);
        writePos += length;
        return p.future();
    }

    @Override
    public Future<Void> write(Buffer buffer, long position) {
        Promise<Void> p = context.promise();
        doWrite(buffer, position, p);
        return p.future();
    }

    // If the Vert.x [Buffer] is backed by a [ByteBuf], this method will not modify the state of it by design.
    // The documentation states that "[BufferImpl] implementation ignores the wrapped [ByteBuf] reader index"
    // but that's not actually true; methods like appendBuffer make explicit use of it. Our only option is to
    // treat [Buffer] as random access and let the caller deal with the Buffer as needed.
    private void doWrite(Buffer buffer, long position, Promise<Void> promise) {
        checkContext();
        checkClosed();

        final ByteBuf bb;
        if (buffer instanceof BufferImpl bufferImpl && bufferImpl.byteBuf().hasMemoryAddress()) {
            bb = bufferImpl.byteBuf().retainedSlice();
        } else {
            bb = Buffers.direct(buffer.length());
            // Might be able to copy efficiently
            if (buffer instanceof BufferInternal bufferInternal) {
                // getByteBuf is an independent view
                bb.writeBytes(bufferInternal.getByteBuf());
            } else {
                // And also might not -_-
                bb.writeBytes(buffer.getBytes());
            }
        }

        int length = bb.readableBytes();

        writesOutstandingBytes += length;
        submitWrite(bb, position, promise);

        promise.future();
    }

    private void submitWrite(final ByteBuf bb, final long position, final Promise<Void> promise) {
        boolean wasFull = writeQueueFull();

        io.netty.util.concurrent.Future<Integer> writeFuture;
        try {
            writeFuture = handle.writeAsync(bb, position, false);
        } catch (Throwable t) {
            bb.release();
            // Only decrement by whatever is in this buffer, which should realistically be equal to length but in the
            // event something was partially written AND failed then we'll see that here.
            writesOutstandingBytes -= bb.readableBytes();
            promise.tryFail(t);
            return;
        }

        writeFuture.addListener(f -> {
            if (!f.isSuccess()) {
                bb.release();
                writesOutstandingBytes -= bb.readableBytes();
                promise.tryFail(f.cause());
                return;
            }

            int res = (int) f.getNow();

            writesOutstandingBytes -= res;
            bb.readerIndex(bb.readerIndex() + res);

            if (bb.isReadable()) {
                submitWrite(bb, position + res, promise);
            } else {
                bb.release();
                try {
                    if (wasFull && !writeQueueFull() && drainHandler != null) {
                        drainHandler.handle(null);
                    }
                    promise.tryComplete();
                } catch (Throwable t) {
                    promise.tryFail(t);
                }
            }
        });
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
        return writesOutstandingBytes >= maxWrites || !handle.permit(NativeConstants.IoRingOp.WRITE);
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
        Promise<Buffer> promise = context.promise();
        doRead(buffer, offset, position, length, promise);
        return promise.future();
    }

    private void doRead(Buffer buffer, int offset, long position, int length, Promise<Buffer> promise) {
        ByteBuf bb;
        ByteBuf slice;

        if (buffer instanceof BufferImpl bufferImpl && bufferImpl.byteBuf().hasMemoryAddress()) {
            bb = bufferImpl.byteBuf().retain();
            slice = bb.slice(offset, length);
            slice.writerIndex(0);
        } else {
            // -_-
            bb = slice = Buffers.direct(length);
        }

        handle.readAsync(slice, position)
            .addListener(f -> {
                if (!f.isSuccess()) {
                    bb.release();
                    promise.fail(f.cause());
                    return;
                }

                int res = (int) f.getNow();

                try {
                    if (bb != slice) {
                        // Zero-copy path, update the write index to offset + whatever we wrote.
                        bb.writerIndex(offset + res);
                    } else {
                        // Copy path. Update indices and write.
                        bb.writerIndex(bb.writerIndex() + res);
                        buffer.setBuffer(offset, BufferInternal.buffer(bb));
                    }
                } finally {
                    bb.release();
                }

                int remaining = length - res;

                if (remaining > 0) {
                    doRead(buffer, offset + res, position + res, remaining, promise);
                } else {
                    promise.complete(buffer);
                }
            });
    }

    @Override
    public Future<Void> flush() {
        checkContext();
        checkClosed();

        Promise<Void> p = context.promise();

        handle
            .fsyncAsync(false, 0, 0)
            .addListener((f) -> {
                if (!f.isSuccess()) p.fail(f.cause());
                else p.complete();
            });

        return p.future();
    }

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
        ByteBuf statBuffer = Buffers.direct(256, true);
        handle
            .statxAsync(NativeConstants.StatxMask.SIZE, 0, statBuffer)
            .addListener((f) -> {
                if (!f.isSuccess()) {
                    statBuffer.release();
                    p.fail(f.cause());
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
            .addListener((closeFuture) -> context.runOnContext(v -> {
                if (!closeFuture.isSuccess()) closeHandlePromise.fail(closeFuture.cause());
                else closeHandlePromise.complete();
            }));

        Promise<Void> closeChannelPromise = context.promise();
        lockChannel
            .onComplete(ar -> {
                if (ar.failed()) closeChannelPromise.fail(ar.cause());
                else {
                    context
                        .executeBlockingInternal(() -> {
                            if (ar.result() != null) {
                                ar.result().close();
                            }
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
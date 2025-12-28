package io.jiouring.file;

import io.netty.buffer.ByteBuf;
import io.netty.channel.IoEventLoop;
import io.netty.channel.unix.IovArray;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.concurrent.CompletableFuture;

public class IoUringFile implements AutoCloseable {

    private final IoUringFileIoHandle ioUringIoHandle;

    private IoUringFile(IoUringFileIoHandle ioUringIoHandle) {
        this.ioUringIoHandle = ioUringIoHandle;
    }

    private void ensureBuffer(ByteBuf buffer, boolean read) {
        if (!buffer.hasMemoryAddress()) {
            throw new IllegalArgumentException("Buffer is not direct");
        }
        if (read && !buffer.isWritable()) {
            throw new IllegalArgumentException("Buffer is not writable");
        }
        if (!read && !buffer.isReadable()) {
            throw new IllegalArgumentException("Buffer is not readable");
        }
    }

    public CompletableFuture<Integer> readAsync(ByteBuf byteBuf, long offset) {
        ensureBuffer(byteBuf, true);
        CompletableFuture<Integer> promise = new UncancellableFuture<>();
        ioUringIoHandle.readAsync(byteBuf.retain(), offset)
            .addListener((f) -> {
                try {
                    if (!f.isSuccess()) {
                        promise.completeExceptionally(f.cause());
                    } else {
                        int res = (int) f.get();
                        byteBuf.writerIndex(byteBuf.writerIndex() + res);
                        promise.complete(res);
                    }
                } catch (Throwable e) {
                    promise.completeExceptionally(e);
                } finally {
                    byteBuf.release();
                }
            });

        return promise;
    }

    public CompletableFuture<Integer> readvAsync(long offset, ByteBuf... buffers) {
        CompletableFuture<Integer> promise = new UncancellableFuture<>();
        IovArray iovArray = createSafeIovArray(buffers, true);

        ioUringIoHandle.readvAsync(iovArray, offset)
            .addListener((f) -> {
                try {
                    iovArray.release();
                    if (!f.isSuccess()) {
                        promise.completeExceptionally(f.cause());
                    } else {
                        int res = (int) f.get();
                        progressBuffers(buffers, res, true);
                        promise.complete(res);
                    }
                } catch (Throwable e) {
                    promise.completeExceptionally(e);
                } finally {
                    for (ByteBuf b : buffers) {
                        b.release();
                    }
                }
            });

        return promise;
    }

    public CompletableFuture<Integer> writeAsync(ByteBuf byteBuf, long offset, boolean dsync) {
        ensureBuffer(byteBuf, false);
        CompletableFuture<Integer> promise = new UncancellableFuture<>();

        ioUringIoHandle.writeAsync(byteBuf.retain(), offset, dsync)
            .addListener((f) -> {
                try {
                    if (!f.isSuccess()) {
                        promise.completeExceptionally(f.cause());
                    } else {
                        int res = (int) f.get();
                        byteBuf.readerIndex(byteBuf.readerIndex() + res);
                        promise.complete(res);
                    }
                } catch (Throwable e) {
                    promise.completeExceptionally(e);
                } finally {
                    byteBuf.release();
                }
            });

        return promise;
    }

    // Overload for dsync=false
    public CompletableFuture<Integer> writeAsync(ByteBuf byteBuf, long offset) {
        return writeAsync(byteBuf, offset, false);
    }

    public CompletableFuture<Integer> writevAsync(long offset, ByteBuf... buffers) {
        CompletableFuture<Integer> promise = new UncancellableFuture<>();
        IovArray iovArray = createSafeIovArray(buffers, false);

        ioUringIoHandle.writevAsync(iovArray, offset)
            .addListener((f) -> {
                try {
                    iovArray.release();
                    if (!f.isSuccess()) {
                        promise.completeExceptionally(f.cause());
                    } else {
                        int res = (int) f.get();
                        progressBuffers(buffers, res, false);
                        promise.complete(res);
                    }
                } catch (Throwable e) {
                    promise.completeExceptionally(e);
                } finally {
                    for (ByteBuf b : buffers) {
                        b.release();
                    }
                }
            });

        return promise;
    }

    public CompletableFuture<Integer> allocate(long offset, long length) {
        return fallocate(offset, length, 0);
    }

    public CompletableFuture<Integer> punchHole(long offset, long length) {
        return fallocate(
            offset,
            length,
            NativeConstants.FallocateFlags.PUNCH_HOLE | NativeConstants.FallocateFlags.KEEP_SIZE
        );
    }

    public CompletableFuture<Integer> fallocate(long offset, long length, int mode) {
        CompletableFuture<Integer> promise = new CompletableFuture<>();
        AsyncUtils.completeFrom(promise, ioUringIoHandle.fallocateAsync(offset, length, mode));
        return promise;
    }

    public CompletableFuture<Integer> truncate(long length) {
        CompletableFuture<Integer> promise = new CompletableFuture<>();
        AsyncUtils.completeFrom(promise, ioUringIoHandle.truncateAsync(length));
        return promise;
    }

    public CompletableFuture<Integer> fsync(boolean isSyncData, int len, long offset) {
        CompletableFuture<Integer> promise = new CompletableFuture<>();
        AsyncUtils.completeFrom(promise, ioUringIoHandle.fsyncAsync(isSyncData, len, offset));
        return promise;
    }

    // Default fsync (entire file, sync metadata too)
    public CompletableFuture<Integer> fsync() {
        return fsync(false, 0, 0);
    }

    // Delete = unlink() + close()
    public CompletableFuture<Integer> delete() {
        CompletableFuture<Integer> promise = new CompletableFuture<>();
        if (ioUringIoHandle.isAnonymous) {
            AsyncUtils.completeFrom(promise, closeAsync());
        } else {
            ioUringIoHandle.unlinkAsync()
                .addListener((f) -> {
                    if (!f.isSuccess()) promise.completeExceptionally(f.cause());
                    else AsyncUtils.completeFrom(promise, closeAsync());
                });
        }

        return promise;
    }

    public CompletableFuture<FileStats> stat() {
        return stat(
            NativeConstants.StatxMask.BASIC_STATS,
            NativeConstants.StatxFlags.AT_SYNC_AS_STAT
        );
    }

    private CompletableFuture<FileStats> stat(int mask, int flags) {
        CompletableFuture<FileStats> promise = new CompletableFuture<>();
        ByteBuf statBuffer = Buffers.pooledDirect(256);
        ioUringIoHandle.statxAsync(mask, flags, statBuffer)
            .addListener((f) -> {
                try {
                    if (!f.isSuccess()) {
                        promise.completeExceptionally(f.cause());
                        return;
                    }

                    try {
                        FileStats stats = new FileStats(statBuffer);
                        promise.complete(stats);
                    } catch (Throwable t) {
                        promise.completeExceptionally(t);
                    }
                } finally {
                    statBuffer.release();
                }
            });

        return promise;
    }

    private IovArray createSafeIovArray(ByteBuf[] buffers, boolean isRead) {
        if (buffers.length == 0) {
            throw new IllegalArgumentException("Buffers empty");
        }

        IovArray iov = new IovArray(buffers.length);
        for (ByteBuf b : buffers) {
            if (!b.hasMemoryAddress()) {
                throw new IllegalArgumentException("Buffer is not direct");
            }

            if (isRead) iov.add(b, b.writerIndex(), b.writableBytes());
            else iov.add(b, b.readerIndex(), b.readableBytes());

            b.retain();
        }
        return iov;
    }

    private void progressBuffers(ByteBuf[] buffers, int syscallResult, boolean isRead) {
        int remaining = syscallResult;

        for (ByteBuf buf : buffers) {
            if (remaining <= 0) break;

            int progress;
            if (isRead) {
                progress = Math.min(buf.writableBytes(), remaining);
                buf.writerIndex(buf.writerIndex() + progress);
            } else {
                progress = Math.min(buf.readableBytes(), remaining);
                buf.readerIndex(buf.readerIndex() + progress);
            }
            remaining -= progress;
        }
    }

    public CompletableFuture<Integer> closeAsync() {
        CompletableFuture<Integer> promise = new UncancellableFuture<>();
        AsyncUtils.completeFrom(promise, ioUringIoHandle.closeAsync());
        return promise;
    }

    @Override
    public void close() {
        ioUringIoHandle.close();
    }

    public static CompletableFuture<IoUringFile> open(
        Path path,
        IoEventLoop ioEventLoop,
        OpenOption[] openOptions,
        FileAttribute<?>... attrs
    ) {
        return IoUringFileIoHandle.open(path, ioEventLoop, openOptions, attrs)
            .thenApply(IoUringFile::new);
    }

    public static CompletableFuture<IoUringFile> open(
        Path path,
        IoEventLoop ioEventLoop,
        OpenOption... openOptions
    ) {
        return open(path, ioEventLoop, openOptions, new FileAttribute[0]);
    }

    public static CompletableFuture<IoUringFile> createTempFile(
        IoEventLoop ioEventLoop,
        OpenOption[] openOptions,
        FileAttribute<?>... attrs
    ) {
        return IoUringFileIoHandle.createTempFile(ioEventLoop, openOptions, attrs)
            .thenApply(IoUringFile::new);
    }

    public static CompletableFuture<IoUringFile> createTempFile(
        IoEventLoop ioEventLoop,
        OpenOption... openOptions
    ) {
        return IoUringFileIoHandle.createTempFile(ioEventLoop, openOptions, new FileAttribute[0])
            .thenApply(IoUringFile::new);
    }
}
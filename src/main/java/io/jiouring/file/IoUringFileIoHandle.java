package io.jiouring.file;

import io.netty.buffer.ByteBuf;
import io.netty.channel.IoEvent;
import io.netty.channel.IoEventLoop;
import io.netty.channel.IoRegistration;
import io.netty.channel.unix.IovArray;
import io.netty.channel.uring.IoUringIoEvent;
import io.netty.channel.uring.IoUringIoHandle;
import io.netty.channel.uring.IoUringIoOps;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class IoUringFileIoHandle implements IoUringIoHandle {

    private static final Logger logger = LogManager.getLogger(IoUringFileIoHandle.class);
    private static final long OP_TIMEOUT_NS = 30_000_000_000L;

    public final Path path;
    private final IoEventLoop ioEventLoop;
    private final int flags;
    private final int mode;

    private final AsyncOpRegistry contextRegistry = new AsyncOpRegistry();

    private State state = State.INITIALIZING;

    private boolean closeSubmitted = false;
    private final AtomicReference<CompletableFuture<Integer>> closeFuture = new AtomicReference<>(null);
    private CompletableFuture<IoUringFileIoHandle> openFuture = null;

    private IoRegistration ioRegistration;
    private int fd = -1;

    public final boolean isAnonymous;
    public final boolean isDirectory;

    private ScheduledFuture<?> stuckOpsCleanerTask;

    private IoUringFileIoHandle(Path path, IoEventLoop ioEventLoop, int flags, int mode) {
        this.path = path;
        this.ioEventLoop = ioEventLoop;
        this.flags = flags;
        this.mode = mode;

        this.isAnonymous = (flags & NativeConstants.OpenFlags.TMPFILE) == NativeConstants.OpenFlags.TMPFILE;
        this.isDirectory = !isAnonymous && ((flags & NativeConstants.OpenFlags.DIRECTORY) == NativeConstants.OpenFlags.DIRECTORY);

        this.stuckOpsCleanerTask = ioEventLoop.scheduleAtFixedRate(
            this::checkStuckOps,
            1,
            1,
            TimeUnit.SECONDS
        );
    }

    public IoUringFileIoHandle init(IoRegistration ioRegistration) {
        if (!ioRegistration.isValid()) {
            throw new IllegalStateException("IoRegistration is not valid");
        }
        this.ioRegistration = ioRegistration;
        this.state = State.INITIALIZED;
        return this;
    }

    private boolean isClosed() {
        return state == State.CLOSING || state == State.CLOSED;
    }

    private void checkStuckOps() {
        List<AsyncOpContext> stuckOps = contextRegistry.findStuckOps(OP_TIMEOUT_NS);

        if (stuckOps.isEmpty()) return;

        logger.warn("Found {} stuck operations. Attempting cleanup.", stuckOps.size());

        for (AsyncOpContext ctx : stuckOps) {
            if (ctx.uringId == -1L) {
                // Never submitted to kernel
                logger.warn("Force releasing unsubmitted op: {}", ctx.id);
                contextRegistry.release(ctx, new TimeoutException("Op never submitted"));
            } else {
                // Kernel has it
                logger.warn("Attempting to cancel stuck op: {}", ctx.id);
                submitCancel(ctx.id);
            }
        }
    }

    private void withEventLoop(Runnable block) {
        if (ioEventLoop.inEventLoop()) {
            block.run();
        } else {
            ioEventLoop.execute(block);
        }
    }

    private SyscallFuture internalSubmit(AsyncOpContext ctx, Function<AsyncOpContext, IoUringIoOps> opFactory) {
        if (state == State.CLOSED) {
            contextRegistry.release(ctx, new IOException("Handle is closed"));
            return ctx.future;
        }

        byte op = ctx.op;

        if (state == State.CLOSING && !(op == NativeConstants.IoRingOp.CLOSE || op == NativeConstants.IoRingOp.ASYNC_CANCEL)) {
            SyscallFuture f = new SyscallFuture();
            f.fail(new IOException("Handle is closing"));
            return f;
        }

        if (!ioRegistration.isValid()) {
            contextRegistry.release(ctx, new IllegalStateException("Registration is invalid"));
            return ctx.future;
        }

        try {
            ctx.uringId = ioRegistration.submit(opFactory.apply(ctx));
            if (ctx.uringId == -1L) {
                contextRegistry.release(ctx, new IOException("io_uring submission failed (ring full?)"));
            }
        } catch (Throwable t) {
            contextRegistry.release(ctx, t);
        }

        return ctx.future;
    }

    private SyscallFuture submitOnLoop(byte opCode, Function<AsyncOpContext, IoUringIoOps> opFactory) {
        if (ioEventLoop.inEventLoop()) {
            if (state == State.CLOSED) {
                SyscallFuture f = new SyscallFuture();
                f.fail(new IOException("Handle is closed"));
                return f;
            }

            if (state == State.CLOSING && !(opCode == NativeConstants.IoRingOp.CLOSE || opCode == NativeConstants.IoRingOp.ASYNC_CANCEL)) {
                SyscallFuture f = new SyscallFuture();
                f.fail(new IOException("Handle is closing"));
                return f;
            }

            if (contextRegistry.isFull()) {
                SyscallFuture f = new SyscallFuture();
                f.fail(new IllegalStateException("Context registry is full"));
                return f;
            }

            AsyncOpContext ctx = contextRegistry.next(opCode);
            return internalSubmit(ctx, opFactory);
        }

        SyscallFuture proxy = new SyscallFuture();
        ioEventLoop.execute(() -> {
            try {
                if (isClosed()) {
                    proxy.fail(new IOException("Handle is closed"));
                    return;
                }

                AsyncOpContext ctx = contextRegistry.next(opCode);
                internalSubmit(ctx, opFactory)
                    .onComplete((res, err) -> {
                        if (err != null) proxy.fail(err);
                        else proxy.complete(res);
                    });
            } catch (Throwable t) {
                proxy.fail(t);
            }
        });

        return proxy;
    }

    private CompletableFuture<IoUringFileIoHandle> open(ByteBuf pathCStr) {
        if (openFuture != null) return openFuture;

        CompletableFuture<IoUringFileIoHandle> proxy = new CompletableFuture<>();
        openFuture = proxy;

        withEventLoop(() -> {
            if (isClosed()) {
                proxy.completeExceptionally(new IOException("IoUringFileIoHandle is " + state + " (op=open)"));
                return;
            }

            state = State.OPENING;

            SyscallFuture f = submitOnLoop(NativeConstants.IoRingOp.OPENAT, ctx ->
                new IoUringIoOps(
                    ctx.op, (byte) 0, (byte) 0, -1,
                    0L, pathCStr.memoryAddress(), mode, flags,
                    ctx.id, (short) 0, (short) 0, 0, 0L
                )
            );

            f.onComplete((res, err) -> {
                if (err != null) {
                    state = State.INITIALIZED;
                    logger.warn("Failed to initialize. Cancelling ioRegistration.");
                    ioRegistration.cancel();
                    proxy.completeExceptionally(err);
                } else {
                    fd = res;
                    state = State.OPEN;
                    proxy.complete(this);
                }
            });
        });

        return proxy;
    }

    public SyscallFuture fallocateAsync(long offset, long length, int mode) {
        return submitOnLoop(NativeConstants.IoRingOp.FALLOCATE, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                offset, length, mode, 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    public SyscallFuture writeAsync(ByteBuf buffer, long offset, boolean dsync) {
        return submitOnLoop(NativeConstants.IoRingOp.WRITE, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                offset, buffer.memoryAddress(), buffer.readableBytes(),
                dsync ? NativeConstants.RwFlags.DSYNC : 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    public SyscallFuture readAsync(ByteBuf buffer, long offset) {
        return submitOnLoop(NativeConstants.IoRingOp.READ, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                offset, buffer.memoryAddress(), buffer.writableBytes(), 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    public SyscallFuture readvAsync(IovArray iovArray, long offset) {
        return submitOnLoop(NativeConstants.IoRingOp.READV, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                offset, iovArray.memoryAddress(0), iovArray.count(), 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    public SyscallFuture writevAsync(IovArray iovArray, long offset) {
        return submitOnLoop(NativeConstants.IoRingOp.WRITEV, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                offset, iovArray.memoryAddress(0), iovArray.count(), 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    // It goes in the offset slot; that's not a mistake
    public SyscallFuture truncateAsync(long length) {
        return submitOnLoop(NativeConstants.IoRingOp.FTRUNCATE, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                length, 0L, 0, 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    public SyscallFuture fsyncAsync(boolean isSyncData, int len, long offset) {
        return submitOnLoop(NativeConstants.IoRingOp.FSYNC, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                offset, 0L, len,
                isSyncData ? NativeConstants.FsyncFlags.DATASYNC : 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    public SyscallFuture unlinkAsync() {
        SyscallFuture proxy = new SyscallFuture();
        ByteBuf pathCStr = OpenHelpers.cStr(path);

        SyscallFuture f = submitOnLoop(NativeConstants.IoRingOp.UNLINKAT, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, -1,
                0L, pathCStr.memoryAddress(), 0,
                isDirectory ? NativeConstants.AtFlags.AT_REMOVEDIR : 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );

        f.onComplete((res, err) -> {
            pathCStr.release();
            if (err != null) proxy.fail(err);
            else proxy.complete(res);
        });

        return proxy;
    }

    public SyscallFuture statxAsync(int mask, int flags, ByteBuf statxBuffer) {
        return submitOnLoop(NativeConstants.IoRingOp.STATX, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                statxBuffer.memoryAddress(), 0L, mask,
                NativeConstants.AtFlags.AT_EMPTY_PATH | flags,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    private void submitCancel(long uringId) {
        if (contextRegistry.isFull()) {
            logger.warn("Registry full; cannot submit cancellation for {}", uringId);
            return;
        }

        submitOnLoop(NativeConstants.IoRingOp.ASYNC_CANCEL, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, -1,
                0L, uringId, 0, 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    private void submitCancelAll() {
        if (contextRegistry.isEmpty()) {
            return;
        }

        if (contextRegistry.isFull()) {
            logger.warn("Registry full; cannot submit cancel all operation");
            return;
        }

        submitOnLoop(NativeConstants.IoRingOp.ASYNC_CANCEL, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                0L, 0L, 0,
                NativeConstants.AsyncCancelFlags.ALL | NativeConstants.AsyncCancelFlags.FD,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    private void submitCloseIfReady() {
        if (state != State.CLOSING || !contextRegistry.isEmpty() || closeSubmitted) return;

        IoUringIoOps ops = new IoUringIoOps(
            NativeConstants.IoRingOp.CLOSE, (byte) 0, (short) 0, fd,
            0L, 0L, 0, 0,
            Short.MAX_VALUE, (short) 0, (short) 0, 0, 0L
        );

        try {
            long uringId = ioRegistration.submit(ops);
            if (uringId == -1L) throw new IOException("Failed to submit close");
            closeSubmitted = true;
        } catch (Throwable t) {
            CompletableFuture<Integer> f = closeFuture.get();
            if (f != null) f.completeExceptionally(t);
            state = State.CLOSED;
        }

        ioRegistration.cancel();
    }

    @Override
    public void handle(IoRegistration ioRegistration, IoEvent ioEvent) {
        IoUringIoEvent event = (IoUringIoEvent) ioEvent;

        // Special case for CLOSE
        if (event.data() == Short.MAX_VALUE && event.opcode() == NativeConstants.IoRingOp.CLOSE) {
            int res = event.res();
            CompletableFuture<Integer> f = closeFuture.get();

            if (f == null) {
                logger.error("Received close event completion but close future isn't set. This should not happen.");
                return;
            }

            if (res < 0) f.completeExceptionally(new IOException("Close failed: " + res));
            else f.complete(res);

            return;
        }

        contextRegistry.complete(event);
        submitCloseIfReady();
    }

    public CompletableFuture<Integer> closeAsync() {
        if (closeFuture.get() != null) return closeFuture.get();

        CompletableFuture<Integer> promise = new CompletableFuture<>();
        if (closeFuture.compareAndSet(null, promise)) {
            withEventLoop(() -> {
                if (stuckOpsCleanerTask != null) {
                    stuckOpsCleanerTask.cancel(false);
                    stuckOpsCleanerTask = null;
                }

                if (state != State.CLOSING && state != State.CLOSED) {
                    state = State.CLOSING;
                    if (ioRegistration != null && ioRegistration.isValid()) {
                        submitCancelAll();
                        submitCloseIfReady();
                    } else {
                        state = State.CLOSED;
                        promise.complete(0);
                    }
                } else {
                    promise.complete(0);
                }
            });
            return promise;
        }

        return closeFuture.get();
    }

    @Override
    public void close() throws Exception {
        closeAsync().join();
    }

    private enum State {
        INITIALIZING,
        INITIALIZED,
        OPENING,
        OPEN,
        CLOSING,
        CLOSED
    }

    public static CompletableFuture<IoUringFileIoHandle> open(
        Path path,
        IoEventLoop ioEventLoop,
        int flags,
        int mode
    ) {
        CompletableFuture<IoUringFileIoHandle> future = new CompletableFuture<>();
        IoUringFileIoHandle handle = new IoUringFileIoHandle(path, ioEventLoop, flags, mode);

        ioEventLoop.register(handle).addListener(f -> {
            if (!f.isSuccess()) {
                future.completeExceptionally(f.cause());
                return;
            }

            ByteBuf pathCStr = OpenHelpers.cStr(path);
            try {
                IoRegistration reg = (IoRegistration) f.get();
                handle.init(reg)
                    .open(pathCStr)
                    .whenComplete((res, err) -> {
                        pathCStr.release();
                        if (err != null) {
                            future.completeExceptionally(err);
                        } else {
                            future.complete(res);
                        }
                    });
            } catch (Throwable t) {
                pathCStr.release();
                future.completeExceptionally(t);
            }
        });

        return future;
    }

    public static CompletableFuture<IoUringFileIoHandle> open(
        Path path,
        IoEventLoop ioEventLoop,
        OpenOption[] options,
        FileAttribute<?>... attrs
    ) {
        if (Files.isDirectory(path)) {
            throw new IllegalArgumentException("file is directory");
        }

        if (!ioEventLoop.isCompatible(IoUringIoHandle.class)) {
            throw new IllegalArgumentException("ioEventLoop is not compatible with IoUringIoHandle");
        }

        int flags = OpenHelpers.openFlags(options);
        int mode = OpenHelpers.fileMode(attrs);

        return open(path, ioEventLoop, flags, mode);
    }

    public static CompletableFuture<IoUringFileIoHandle> open(
        Path path,
        IoEventLoop ioEventLoop,
        OpenOption... options
    ) {
        return open(path, ioEventLoop, options, new FileAttribute[0]);
    }

    public static CompletableFuture<IoUringFileIoHandle> createTempFile(
        IoEventLoop ioEventLoop,
        OpenOption[] options,
        FileAttribute<?>... attrs
    ) {
        if (!ioEventLoop.isCompatible(IoUringIoHandle.class)) {
            throw new IllegalArgumentException("ioEventLoop is not compatible with IoUringIoHandle");
        }

        Path tmpDir = Paths.get(System.getProperty("java.io.tmpdir"));
        int userFlags = OpenHelpers.openFlags(options);

        boolean hasWrite = (userFlags & NativeConstants.OpenFlags.WRONLY) != 0 ||
            (userFlags & NativeConstants.OpenFlags.RDWR) != 0;

        int mandatoryAccess = hasWrite ? 0 : NativeConstants.OpenFlags.RDWR;

        int finalFlags = userFlags | NativeConstants.OpenFlags.TMPFILE | mandatoryAccess;

        int mode = OpenHelpers.fileMode(attrs);
        if (mode == NativeConstants.FileMode.DEFAULT_FILE && attrs.length == 0) {
            mode = NativeConstants.FileMode.S_IRUSR | NativeConstants.FileMode.S_IWUSR;
        }

        return open(tmpDir, ioEventLoop, finalFlags, mode);
    }

    public static CompletableFuture<IoUringFileIoHandle> createTempFile(
        IoEventLoop ioEventLoop,
        OpenOption... openOptions
    ) {
        return createTempFile(ioEventLoop, openOptions, new FileAttribute[0]);
    }
}
package io.jiouring.file;

import io.netty.buffer.ByteBuf;
import io.netty.channel.IoEvent;
import io.netty.channel.IoEventLoop;
import io.netty.channel.IoRegistration;
import io.netty.channel.unix.IovArray;
import io.netty.channel.uring.IoUringIoEvent;
import io.netty.channel.uring.IoUringIoHandle;
import io.netty.channel.uring.IoUringIoOps;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class IoUringFileIoHandle implements IoUringIoHandle {

    private static final Logger logger = LoggerFactory.getLogger(IoUringFileIoHandle.class);
    private static final long OP_TIMEOUT_NS = 30_000_000_000L;

    public final Path path;
    private final IoEventLoop ioEventLoop;
    private final int flags;
    private final int mode;

    private final AsyncOpRegistry contextRegistry;

    private State state = State.INITIALIZING;
    private boolean closeSubmitted = false;

    private final AtomicReference<Promise<IoUringFileIoHandle>> openPromise = new AtomicReference<>(null);
    private final AtomicReference<Promise<Integer>> closePromise = new AtomicReference<>(null);

    private IoRegistration ioRegistration;
    private int fd = -1;

    public final boolean isAnonymous;
    public final boolean isDirectory;

    private final ScheduledFuture<?> stuckOpsCleanerTask;

    private IoUringFileIoHandle(Path path, IoEventLoop ioEventLoop, int flags, int mode) {
        this.path = path;
        this.ioEventLoop = ioEventLoop;
        this.flags = flags;
        this.mode = mode;
        this.contextRegistry = new AsyncOpRegistry(ioEventLoop);

        this.isAnonymous = (flags & NativeConstants.OpenFlags.TMPFILE) == NativeConstants.OpenFlags.TMPFILE;
        this.isDirectory = !isAnonymous && ((flags & NativeConstants.OpenFlags.DIRECTORY) == NativeConstants.OpenFlags.DIRECTORY);

        this.stuckOpsCleanerTask = ioEventLoop.scheduleAtFixedRate(
            this::checkStuckOps,
            1,
            1,
            TimeUnit.SECONDS
        );
    }

    public boolean canWrite() {
        return contextRegistry.canAcquire(NativeConstants.IoRingOp.WRITE);
    }

    public IoUringFileIoHandle init(IoRegistration ioRegistration) {
        if (!ioRegistration.isValid()) {
            throw new IllegalStateException("IoRegistration is not valid");
        }
        this.ioRegistration = ioRegistration;
        this.state = State.INITIALIZED;
        return this;
    }

    private void checkStuckOps() {
        List<AsyncOpContext> stuckOps = contextRegistry.findStuckOps(OP_TIMEOUT_NS);
        if (stuckOps.isEmpty()) {
            return;
        }

        logger.warn("Found {} stuck operations. Attempting cleanup.", stuckOps.size());

        for (AsyncOpContext ctx : stuckOps) {
            logger.warn("Attempting to cancel stuck op: {}", ctx);
            cancelAsync(ctx.uringId);
        }
    }

    private Future<Integer> submit(byte op, Function<AsyncOpContext, IoUringIoOps> factory) {
        if (!ioRegistration.isValid()) {
            return ioEventLoop.newFailedFuture(
                new IOException("Registration is invalid")
            );
        }

        if (state == State.FAILED || state == State.CLOSED) {
            return ioEventLoop.newFailedFuture(
                new IOException("Handle is in terminal state: " + state)
            );
        }

        if (state == State.CLOSING && !(op == NativeConstants.IoRingOp.CLOSE || op == NativeConstants.IoRingOp.ASYNC_CANCEL)) {
            return ioEventLoop.newFailedFuture(
                new IOException("Handle is closing")
            );
        }

        if (!contextRegistry.canAcquire(op)) {
            return ioEventLoop.newFailedFuture(
                new IOException("Context registry is full for " + op)
            );
        }

        AsyncOpContext ctx = null;
        try {
            ctx = contextRegistry.acquire(op);
            ctx.uringId = ioRegistration.submit(factory.apply(ctx));
            if (ctx.uringId == -1L) throw new IOException("io_uring submission failed (ring full?)");
            return ctx.future;
        } catch (Throwable t) {
            if (ctx != null) contextRegistry.release(ctx, t);
            return ioEventLoop.newFailedFuture(t);
        }
    }

    private Future<Integer> safeSubmit(byte op, Function<AsyncOpContext, IoUringIoOps> factory) {
        try {
            if (!ioEventLoop.inEventLoop()) {
                Promise<Integer> proxy = new NettySyscallFuture(ioEventLoop, op);
                ioEventLoop.execute(() -> AsyncUtils.completeFrom(proxy, safeSubmit(op, factory)));
                return proxy;
            }

            return submit(op, factory);
        } catch (Throwable t) {
            return ioEventLoop.newFailedFuture(t);
        }
    }

    private Future<IoUringFileIoHandle> open(ByteBuf pathCStr) {
        Promise<IoUringFileIoHandle> current = openPromise.get();
        if (current != null) {
            return current;
        }

        Promise<IoUringFileIoHandle> promise = ioEventLoop.newPromise();
        if ((current = openPromise.compareAndExchange(null, promise)) != null) {
            return current;
        }

        state = State.OPENING;

        Future<Integer> f = safeSubmit(NativeConstants.IoRingOp.OPENAT, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, -1,
                0L, pathCStr.memoryAddress(), mode, flags,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );

        f.addListener((fut) -> {
            if (!fut.isSuccess()) {
                state = State.FAILED;
                // We only have to cancel the registration. There can't be any other ops in flight right now
                // because the openFuture hasn't completed.
                ioRegistration.cancel();
                promise.tryFailure(fut.cause());
            } else {
                fd = (int) fut.get();
                state = State.OPEN;
                promise.trySuccess(this);
            }
        });

        return promise;
    }

    public Future<Integer> fallocateAsync(long offset, long length, int mode) {
        return safeSubmit(NativeConstants.IoRingOp.FALLOCATE, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                offset, length, mode, 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    public Future<Integer> writeAsync(ByteBuf buffer, long offset, boolean dsync) {
        return safeSubmit(NativeConstants.IoRingOp.WRITE, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                offset, buffer.memoryAddress(), buffer.readableBytes(),
                dsync ? NativeConstants.RwFlags.DSYNC : 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    public Future<Integer> readAsync(ByteBuf buffer, long offset) {
        return safeSubmit(NativeConstants.IoRingOp.READ, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                offset, buffer.memoryAddress(), buffer.writableBytes(), 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    public Future<Integer> readvAsync(IovArray iovArray, long offset) {
        return safeSubmit(NativeConstants.IoRingOp.READV, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                offset, iovArray.memoryAddress(0), iovArray.count(), 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    public Future<Integer> writevAsync(IovArray iovArray, long offset) {
        return safeSubmit(NativeConstants.IoRingOp.WRITEV, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                offset, iovArray.memoryAddress(0), iovArray.count(), 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    // It goes in the offset slot; that's not a mistake
    public Future<Integer> truncateAsync(long length) {
        return safeSubmit(NativeConstants.IoRingOp.FTRUNCATE, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                length, 0L, 0, 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    public Future<Integer> fsyncAsync(boolean isSyncData, int len, long offset) {
        return safeSubmit(NativeConstants.IoRingOp.FSYNC, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                offset, 0L, len,
                isSyncData ? NativeConstants.FsyncFlags.DATASYNC : 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    public Future<Integer> unlinkAsync() {
        ByteBuf pathCStr = OpenHelpers.cStr(path);
        Future<Integer> f = safeSubmit(NativeConstants.IoRingOp.UNLINKAT, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, -1,
                0L, pathCStr.memoryAddress(), 0,
                isDirectory ? NativeConstants.AtFlags.AT_REMOVEDIR : 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
        f.addListener((ignored) -> pathCStr.release());
        return f;
    }

    public Future<Integer> statxAsync(int mask, int flags, ByteBuf statxBuffer) {
        ByteBuf pathCStr = OpenHelpers.cStr(path);
        Future<Integer> f = safeSubmit(NativeConstants.IoRingOp.STATX, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, NativeConstants.AtFlags.AT_FDCWD,
                statxBuffer.memoryAddress(), pathCStr.memoryAddress(), mask, flags,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
        f.addListener((ignored) -> pathCStr.release());
        return f;
    }

    private void cancelAsync(long uringId) {
        safeSubmit(NativeConstants.IoRingOp.ASYNC_CANCEL, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, -1,
                0L, uringId, 0, 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    private Future<Integer> submitCancelAll() {
        if (contextRegistry.isEmpty()) {
            return ioEventLoop.newSucceededFuture(0);
        }

        return safeSubmit(NativeConstants.IoRingOp.ASYNC_CANCEL, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                0L, 0L, 0,
                NativeConstants.AsyncCancelFlags.ALL | NativeConstants.AsyncCancelFlags.FD,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );
    }

    private void maybeSubmitClose() {
        if (state != State.CLOSING || !contextRegistry.isEmpty() || closeSubmitted) {
            return;
        }

        Future<Integer> f = safeSubmit(NativeConstants.IoRingOp.CLOSE, ctx ->
            new IoUringIoOps(
                ctx.op, (byte) 0, (byte) 0, fd,
                0L, 0L, 0, 0,
                ctx.id, (short) 0, (short) 0, 0, 0L
            )
        );

        closeSubmitted = true;

        f.addListener((fut) -> {
            state = State.CLOSED;
            ioRegistration.cancel();

            // closePromise cannot be null if state == CLOSING
            AsyncUtils.completeFrom(closePromise.get(), fut);
        });
    }

    @Override
    public void handle(IoRegistration ioRegistration, IoEvent ioEvent) {
        IoUringIoEvent event = (IoUringIoEvent) ioEvent;
        contextRegistry.complete(event);
        maybeSubmitClose();
    }

    public Future<Integer> closeAsync() {
        if (state == State.FAILED || state == State.CLOSED) {
            logger.debug("Close called on handle in state {}", state);
            return ioEventLoop.newSucceededFuture(0);
        }

        Promise<Integer> current = closePromise.get();
        if (current != null) {
            return current;
        }

        Promise<Integer> promise = ioEventLoop.newPromise();
        if ((current = closePromise.compareAndExchange(null, promise)) != null) {
            return current;
        }

        // We won; set the state to CLOSING
        state = State.CLOSING;

        Runnable closeRunnable = () -> {
            if (!stuckOpsCleanerTask.isDone()) stuckOpsCleanerTask.cancel(false);
            stuckOpsCleanerTask
                .addListener(f -> submitCancelAll()
                    .addListener((ignored) ->
                        maybeSubmitClose()));
        };

        if (ioEventLoop.inEventLoop()) closeRunnable.run();
        else ioEventLoop.execute(closeRunnable);

        return promise;
    }

    @Override
    public void close() {
        closeAsync().syncUninterruptibly();
    }

    private enum State {
        INITIALIZING,
        INITIALIZED,
        OPENING,
        OPEN,
        CLOSING,
        CLOSED,
        FAILED
    }

    public static CompletableFuture<IoUringFileIoHandle> open(
        Path path,
        IoEventLoop ioEventLoop,
        int flags,
        int mode
    ) {
        CompletableFuture<IoUringFileIoHandle> future = new CompletableFuture<>();
        IoUringFileIoHandle handle = new IoUringFileIoHandle(path, ioEventLoop, flags, mode);

        ioEventLoop
            .register(handle)
            .addListener(f -> {
                if (!f.isSuccess()) {
                    future.completeExceptionally(f.cause());
                    return;
                }

                ByteBuf pathCStr = OpenHelpers.cStr(path);
                try {
                    IoRegistration reg = (IoRegistration) f.get();
                    handle
                        .init(reg)
                        .open(pathCStr) // TODO: Should open retain the ByteBuf to avoid the double release logic?
                        .addListener((openFuture) -> {
                            pathCStr.release();
                            if (!openFuture.isSuccess()) future.completeExceptionally(openFuture.cause());
                            else future.complete((IoUringFileIoHandle) openFuture.getNow());
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
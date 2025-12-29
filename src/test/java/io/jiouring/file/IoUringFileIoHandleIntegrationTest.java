package io.jiouring.file;

import io.jiouring.utils.RequiresKernel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.IoEventLoop;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.unix.IovArray;
import io.netty.channel.uring.IoUringIoHandler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.UUID;

import static java.nio.file.StandardOpenOption.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(60)
class IoUringFileIoHandleIntegrationTest {

    private static MultiThreadIoEventLoopGroup group;
    private static IoEventLoop eventLoop;
    private static Path tempDir;

    private Path testPath;
    private IoUringFileIoHandle handle;

    @BeforeAll
    static void initGroup() {
        group = new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());
        eventLoop = group.next();
        tempDir = Path.of(System.getProperty("java.io.tmpdir"));
    }

    @AfterAll
    static void shutdownGroup() throws Exception {
        group.shutdownGracefully().sync();
    }

    @BeforeEach
    void createTestPath() {
        testPath = tempDir.resolve("uring-handle-test-" + UUID.randomUUID() + ".dat");
    }

    @AfterEach
    void cleanup() throws Exception {
        if (handle != null) {
            try {
                handle.close();
            } catch (Exception ignored) {}
        }
        Files.deleteIfExists(testPath);
    }

    private IoUringFileIoHandle openHandle(StandardOpenOption... options) throws Exception {
        handle = IoUringFileIoHandle.open(testPath, eventLoop, options).get();
        return handle;
    }

    @Test
    void openAndClose() throws Exception {
        handle = IoUringFileIoHandle.open(testPath, eventLoop, READ, WRITE, CREATE).get();
        assertNotNull(handle);
        assertEquals(testPath, handle.path);
        assertFalse(handle.isAnonymous);
        assertFalse(handle.isDirectory);
        handle.closeAsync().get();
    }

    @Test
    void writeAsyncWithJoin() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "syscall-future-test".getBytes(StandardCharsets.UTF_8);
        ByteBuf buf = Buffers.direct(data);

        int written = handle.writeAsync(buf, 0, false).sync().getNow();
        assertEquals(data.length, written);

        buf.release();
    }

    @Test
    void readAsyncWithJoin() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "read-test".getBytes(StandardCharsets.UTF_8);
        ByteBuf writeBuf = Buffers.direct(data);
        handle.writeAsync(writeBuf, 0, false).sync().getNow();

        ByteBuf readBuf = Buffers.direct(data.length);
        int read = handle.readAsync(readBuf, 0).sync().getNow();
        assertEquals(data.length, read);

        writeBuf.release();
        readBuf.release();
    }

    @Test
    void writeAsyncWithDsync() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "dsync-write".getBytes(StandardCharsets.UTF_8);
        ByteBuf buf = Buffers.direct(data);

        int written = handle.writeAsync(buf, 0, true).sync().getNow();
        assertEquals(data.length, written);

        buf.release();
    }

    @Test
    void writevAsync() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        ByteBuf buf1 = Buffers.direct("AAA".getBytes(StandardCharsets.UTF_8));
        ByteBuf buf2 = Buffers.direct("BBB".getBytes(StandardCharsets.UTF_8));

        IovArray iov = new IovArray(2);
        iov.add(buf1, buf1.readerIndex(), buf1.readableBytes());
        iov.add(buf2, buf2.readerIndex(), buf2.readableBytes());

        int written = handle.writevAsync(iov, 0).sync().getNow();
        assertEquals(6, written);

        iov.release();
        buf1.release();
        buf2.release();
    }

    @Test
    void readvAsync() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "XXXYYY".getBytes(StandardCharsets.UTF_8);
        ByteBuf writeBuf = Buffers.direct(data);
        handle.writeAsync(writeBuf, 0, false).sync().getNow();

        ByteBuf buf1 = Buffers.direct(3);
        ByteBuf buf2 = Buffers.direct(3);

        IovArray iov = new IovArray(2);
        iov.add(buf1, buf1.writerIndex(), buf1.writableBytes());
        iov.add(buf2, buf2.writerIndex(), buf2.writableBytes());

        int read = handle.readvAsync(iov, 0).sync().getNow();
        assertEquals(6, read);

        iov.release();
        writeBuf.release();
        buf1.release();
        buf2.release();
    }

    @Test
    void fallocateAsync() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        int result = handle.fallocateAsync(0, 4096, 0).sync().getNow();
        assertEquals(0, result);

        ByteBuf statBuf = Buffers.direct(256);
        handle.statxAsync(NativeConstants.StatxMask.STATX_SIZE, 0, statBuf).sync().getNow();
        FileStats stats = new FileStats(statBuf);
        assertTrue(stats.size >= 4096);

        statBuf.release();
    }

    @Test
    @RequiresKernel("6.9")
    void truncateAsync() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = new byte[1000];
        Arrays.fill(data, (byte) 'X');
        ByteBuf buf = Buffers.direct(data);
        handle.writeAsync(buf, 0, false).sync().getNow();

        int result = handle.truncateAsync(500).sync().getNow();
        assertEquals(0, result);

        ByteBuf statBuf = Buffers.direct(256);
        handle.statxAsync(NativeConstants.StatxMask.STATX_SIZE, 0, statBuf).sync().getNow();
        FileStats stats = new FileStats(statBuf);
        assertEquals(500, stats.size);

        buf.release();
        statBuf.release();
    }

    @Test
    void fsyncAsync() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "fsync-test".getBytes(StandardCharsets.UTF_8);
        ByteBuf buf = Buffers.direct(data);
        handle.writeAsync(buf, 0, false).sync().getNow();

        int result = handle.fsyncAsync(false, 0, 0).sync().getNow();
        assertEquals(0, result);

        buf.release();
    }

    @Test
    void fsyncAsyncDataOnly() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "fdatasync".getBytes(StandardCharsets.UTF_8);
        ByteBuf buf = Buffers.direct(data);
        handle.writeAsync(buf, 0, false).sync().getNow();

        int result = handle.fsyncAsync(true, 0, 0).sync().getNow();
        assertEquals(0, result);

        buf.release();
    }

    @Test
    void statxAsync() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = new byte[1234];
        ByteBuf writeBuf = Buffers.direct(data);
        writeBuf.writerIndex(data.length);
        handle.writeAsync(writeBuf, 0, false).sync().getNow();

        ByteBuf statBuf = Buffers.direct(256);
        int result = handle.statxAsync(NativeConstants.StatxMask.STATX_BASIC_STATS, 0, statBuf).sync().getNow();
        assertEquals(0, result);

        FileStats stats = new FileStats(statBuf);
        assertEquals(1234, stats.size);
        assertTrue(stats.nlink >= 1);

        writeBuf.release();
        statBuf.release();
    }

    @Test
    void unlinkAsync() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);
        assertTrue(Files.exists(testPath));

        int result = handle.unlinkAsync().sync().getNow();
        assertEquals(0, result);

        assertFalse(Files.exists(testPath));
    }

    @Test
    void createTempFileIsAnonymous() throws Exception {
        handle = IoUringFileIoHandle.createTempFile(eventLoop, READ, WRITE).get();
        assertTrue(handle.isAnonymous);
    }

    @Test
    void createTempFileCanWriteAndRead() throws Exception {
        handle = IoUringFileIoHandle.createTempFile(eventLoop, READ, WRITE).get();

        byte[] data = "temp-data".getBytes(StandardCharsets.UTF_8);
        ByteBuf writeBuf = Buffers.direct(data);
        int written = handle.writeAsync(writeBuf, 0, false).sync().getNow();
        assertEquals(data.length, written);

        ByteBuf readBuf = Buffers.direct(data.length);
        int read = handle.readAsync(readBuf, 0).sync().getNow();
        assertEquals(data.length, read);

        writeBuf.release();
        readBuf.release();
    }

    @Test
    void onCompleteCallback() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "callback-test".getBytes(StandardCharsets.UTF_8);
        ByteBuf buf = Buffers.direct(data);

        int[] resultHolder = new int[1];
        Throwable[] errorHolder = new Throwable[1];
        Object lock = new Object();

        handle
            .writeAsync(buf, 0, false)
            .addListener((f) -> {
                synchronized (lock) {
                    resultHolder[0] = (int) f.get();
                    errorHolder[0] = f.cause();
                    lock.notify();
                }
            });

        synchronized (lock) {
            if (resultHolder[0] == 0 && errorHolder[0] == null) {
                lock.wait(5000);
            }
        }

        assertEquals(data.length, resultHolder[0]);
        assertNull(errorHolder[0]);

        buf.release();
    }

    @Test
    void negativeResultCreatesException() throws Exception {
        openHandle(READ, CREATE);

        ByteBuf buf = Buffers.direct(10);
        buf.writerIndex(10);

        assertThrows(IOException.class, () -> handle.writeAsync(buf, 0, false).sync().getNow());

        buf.release();
    }

    @Test
    void multipleCloseCallsSucceed() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        handle.closeAsync().get();
        handle.closeAsync().get();
        handle.closeAsync().get();
    }

    @Test
    void closeBlockingMethod() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);
        handle.close();
    }

    @Test
    void operationsAfterCloseThrow() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);
        handle.closeAsync().get();

        ByteBuf buf = Buffers.direct(10);
        buf.writerIndex(10);

        assertThrows(IOException.class, () -> handle.writeAsync(buf, 0, false).sync().getNow());

        buf.release();
    }

    @Test
    void openDirectoryThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            IoUringFileIoHandle.open(tempDir, eventLoop, READ).get());
    }

    @Test
    void pathAccessible() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);
        assertEquals(testPath, handle.path);
    }

    @Test
    void largeFileOperations() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        int size = 1024 * 1024;
        byte[] data = new byte[size];
        Arrays.fill(data, (byte) 'Z');
        ByteBuf buf = Buffers.direct(data);

        int written = handle.writeAsync(buf, 0, false).sync().getNow();
        assertEquals(size, written);

        ByteBuf statBuf = Buffers.direct(256);
        handle.statxAsync(NativeConstants.StatxMask.STATX_SIZE, 0, statBuf).sync().getNow();
        FileStats stats = new FileStats(statBuf);
        assertEquals(size, stats.size);

        buf.release();
        statBuf.release();
    }

    @Test
    void punchHole() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = new byte[8192];
        Arrays.fill(data, (byte) 'P');
        ByteBuf buf = Buffers.direct(data);
        handle.writeAsync(buf, 0, false).sync().getNow();

        int mode = NativeConstants.FallocateFlags.FALLOC_FL_PUNCH_HOLE | NativeConstants.FallocateFlags.FALLOC_FL_KEEP_SIZE;
        int result = handle.fallocateAsync(4096, 4096, mode).sync().getNow();
        assertEquals(0, result);

        ByteBuf statBuf = Buffers.direct(256);
        handle.statxAsync(NativeConstants.StatxMask.STATX_SIZE, 0, statBuf).sync().getNow();
        FileStats stats = new FileStats(statBuf);
        assertEquals(8192, stats.size);

        buf.release();
        statBuf.release();
    }

    @Test
    void writeAtOffset() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] first = "XXXX".getBytes(StandardCharsets.UTF_8);
        byte[] second = "YYYY".getBytes(StandardCharsets.UTF_8);

        ByteBuf buf1 = Buffers.direct(first);
        ByteBuf buf2 = Buffers.direct(second);

        handle.writeAsync(buf1, 0, false).sync().getNow();
        handle.writeAsync(buf2, 100, false).sync().getNow();

        ByteBuf statBuf = Buffers.direct(256);
        handle.statxAsync(NativeConstants.StatxMask.STATX_SIZE, 0, statBuf).sync().getNow();
        FileStats stats = new FileStats(statBuf);
        assertEquals(104, stats.size);

        buf1.release();
        buf2.release();
        statBuf.release();
    }

    @Test
    void readFromOffset() throws Exception {
        openHandle(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "0123456789".getBytes(StandardCharsets.UTF_8);
        ByteBuf writeBuf = Buffers.direct(data);
        handle.writeAsync(writeBuf, 0, false).sync().getNow();

        ByteBuf readBuf = Buffers.direct(3);
        int read = handle.readAsync(readBuf, 5).sync().getNow();
        assertEquals(3, read);

        writeBuf.release();
        readBuf.release();
    }
}

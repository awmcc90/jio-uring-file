package io.jiouring.file;

import io.jiouring.utils.RequiresKernel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.IoEventLoop;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.uring.IoUringIoHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.nio.file.StandardOpenOption.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(60)
class IoUringFileIntegrationTest {

    private static final Logger logger = LogManager.getLogger(IoUringFileIntegrationTest.class);

    private static MultiThreadIoEventLoopGroup group;
    private static IoEventLoop eventLoop;
    private static Path tempDir;

    private Path testPath;
    private IoUringFile file;

    @BeforeAll
    static void initGroup() {
        group = new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());
        eventLoop = group.next();
        tempDir = Path.of(System.getProperty("java.io.tmpdir"));
    }

    @AfterAll
    static void shutdownGroup() throws Exception {
        group.shutdownGracefully().syncUninterruptibly().get();
    }

    @BeforeEach
    void createTestPath() {
        testPath = tempDir.resolve("uring-test-" + UUID.randomUUID() + ".dat");
    }

    @AfterEach
    void cleanup() throws IOException {
        if (file != null) {
            try {
                file.closeAsync().get();
            } catch (Exception ex) {
                logger.error("failed to delete file", ex);
            }
        }
        // TODO: Replace with file.delete() once that handles missing files correctly
        Files.deleteIfExists(testPath);
    }

    private IoUringFile openFile(StandardOpenOption... options) throws Exception {
        file = IoUringFile.open(testPath, eventLoop, options).get();
        return file;
    }

    @Test
    void openAndClose() throws ExecutionException, InterruptedException {
        file = IoUringFile.open(testPath, eventLoop, READ, WRITE, CREATE).get();
        assertNotNull(file);
        file.closeAsync().get();
    }

    @Test
    void writeAndRead() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "hello-io-uring".getBytes(StandardCharsets.UTF_8);
        ByteBuf writeBuf = Buffers.direct(data);
        ByteBuf readBuf = Buffers.direct(data.length);

        int written = file.writeAsync(writeBuf, 0).get();
        assertEquals(data.length, written);

        int read = file.readAsync(readBuf, 0).get();
        assertEquals(data.length, read);

        byte[] result = new byte[data.length];
        readBuf.readBytes(result);
        assertArrayEquals(data, result);

        writeBuf.release();
        readBuf.release();
    }

    @Test
    void writeWithDsync() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "dsync-data".getBytes(StandardCharsets.UTF_8);
        ByteBuf buf = Buffers.direct(data);

        int written = file.writeAsync(buf, 0, true).get();
        assertEquals(data.length, written);

        buf.release();
    }

    @Test
    void writeAtOffset() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] first = "AAAA".getBytes(StandardCharsets.UTF_8);
        byte[] second = "BBBB".getBytes(StandardCharsets.UTF_8);

        ByteBuf buf1 = Buffers.direct(first);
        ByteBuf buf2 = Buffers.direct(second);

        file.writeAsync(buf1, 0).get();
        file.writeAsync(buf2, 4).get();

        ByteBuf readBuf = Buffers.direct(8);
        file.readAsync(readBuf, 0).get();

        byte[] result = new byte[8];
        readBuf.readBytes(result);
        assertArrayEquals("AAAABBBB".getBytes(StandardCharsets.UTF_8), result);

        buf1.release();
        buf2.release();
        readBuf.release();
    }

    @Test
    void readAtOffset() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "0123456789".getBytes(StandardCharsets.UTF_8);
        ByteBuf writeBuf = Buffers.direct(data);
        file.writeAsync(writeBuf, 0).get();

        ByteBuf readBuf = Buffers.direct(4);
        int read = file.readAsync(readBuf, 3).get();
        assertEquals(4, read);

        byte[] result = new byte[4];
        readBuf.readBytes(result);
        assertArrayEquals("3456".getBytes(StandardCharsets.UTF_8), result);

        writeBuf.release();
        readBuf.release();
    }

    @Test
    void writevMultipleBuffers() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        ByteBuf buf1 = Buffers.direct("AAA".getBytes(StandardCharsets.UTF_8));
        ByteBuf buf2 = Buffers.direct("BBB".getBytes(StandardCharsets.UTF_8));
        ByteBuf buf3 = Buffers.direct("CCC".getBytes(StandardCharsets.UTF_8));

        int written = file.writevAsync(0, buf1, buf2, buf3).get();
        assertEquals(9, written);

        ByteBuf readBuf = Buffers.direct(9);
        file.readAsync(readBuf, 0).get();

        byte[] result = new byte[9];
        readBuf.readBytes(result);
        assertArrayEquals("AAABBBCCC".getBytes(StandardCharsets.UTF_8), result);

        buf1.release();
        buf2.release();
        buf3.release();
        readBuf.release();
    }

    @Test
    void readvMultipleBuffers() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "XXXYYYZZZ".getBytes(StandardCharsets.UTF_8);
        ByteBuf writeBuf = Buffers.direct(data);
        file.writeAsync(writeBuf, 0).get();

        ByteBuf buf1 = Buffers.direct(3);
        ByteBuf buf2 = Buffers.direct(3);
        ByteBuf buf3 = Buffers.direct(3);

        int read = file.readvAsync(0, buf1, buf2, buf3).get();
        assertEquals(9, read);

        byte[] r1 = new byte[3];
        byte[] r2 = new byte[3];
        byte[] r3 = new byte[3];
        buf1.readBytes(r1);
        buf2.readBytes(r2);
        buf3.readBytes(r3);

        assertArrayEquals("XXX".getBytes(StandardCharsets.UTF_8), r1);
        assertArrayEquals("YYY".getBytes(StandardCharsets.UTF_8), r2);
        assertArrayEquals("ZZZ".getBytes(StandardCharsets.UTF_8), r3);

        writeBuf.release();
        buf1.release();
        buf2.release();
        buf3.release();
    }

    @Test
    void allocateExtendsFile() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        file.allocate(0, 4096).get();

        FileStats stats = file.stat().get();
        assertTrue(stats.size >= 4096);
    }

    @Test
    @RequiresKernel("6.9")
    void truncateReducesFileSize() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = new byte[1000];
        Arrays.fill(data, (byte) 'X');
        ByteBuf buf = Buffers.direct(data);
        file.writeAsync(buf, 0).get();

        file.truncate(500).get();

        FileStats stats = file.stat().get();
        assertEquals(500, stats.size);

        buf.release();
    }

    @Test
    void fsyncCompletesSuccessfully() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "fsync-test".getBytes(StandardCharsets.UTF_8);
        ByteBuf buf = Buffers.direct(data);
        file.writeAsync(buf, 0).get();

        int result = file.fsync().get();
        assertEquals(0, result);

        buf.release();
    }

    @Test
    void fsyncDataOnly() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "fdatasync-test".getBytes(StandardCharsets.UTF_8);
        ByteBuf buf = Buffers.direct(data);
        file.writeAsync(buf, 0).get();

        int result = file.fsync(true, 0, 0).get();
        assertEquals(0, result);

        buf.release();
    }

    @Test
    void statReturnsFileInfo() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = new byte[1234];
        ByteBuf buf = Buffers.direct(data);
        buf.writerIndex(data.length);
        file.writeAsync(buf, 0).get();

        FileStats stats = file.stat().get();
        assertEquals(1234, stats.size);
        assertTrue(stats.nlink >= 1);

        buf.release();
    }

    @Test
    void deleteRemovesFile() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);
        assertTrue(Files.exists(testPath));

        file.delete().get();
        file = null;

        assertFalse(Files.exists(testPath));
    }

    @Test
    void createTempFile() throws Exception {
        file = IoUringFile.createTempFile(eventLoop, READ, WRITE).get();
        assertNotNull(file);

        byte[] data = "temp-file-data".getBytes(StandardCharsets.UTF_8);
        ByteBuf buf = Buffers.direct(data);
        file.writeAsync(buf, 0).get();

        ByteBuf readBuf = Buffers.direct(data.length);
        file.readAsync(readBuf, 0).get();

        byte[] result = new byte[data.length];
        readBuf.readBytes(result);
        assertArrayEquals(data, result);

        buf.release();
        readBuf.release();
    }

    @Test
    void nonDirectBufferThrowsOnRead() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        ByteBuf heapBuf = Unpooled.buffer(10);
        assertThrows(IllegalArgumentException.class, () ->
            file.readAsync(heapBuf, 0));
        heapBuf.release();
    }

    @Test
    void nonDirectBufferThrowsOnWrite() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        ByteBuf heapBuf = Unpooled.buffer(10);
        heapBuf.writeBytes("test".getBytes());
        assertThrows(IllegalArgumentException.class, () ->
            file.writeAsync(heapBuf, 0));
        heapBuf.release();
    }

    @Test
    void nonWritableBufferThrowsOnRead() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        ByteBuf buf = Buffers.direct(10);
        buf.writerIndex(10);
        assertThrows(IllegalArgumentException.class, () ->
            file.readAsync(buf, 0));
        buf.release();
    }

    @Test
    void nonReadableBufferThrowsOnWrite() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        ByteBuf buf = Buffers.direct(10);
        assertThrows(IllegalArgumentException.class, () ->
            file.writeAsync(buf, 0));
        buf.release();
    }

    @Test
    void emptyBufferArrayThrowsOnWritev() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);
        assertThrows(IllegalArgumentException.class, () ->
            file.writevAsync(0));
    }

    @Test
    void emptyBufferArrayThrowsOnReadv() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);
        assertThrows(IllegalArgumentException.class, () ->
            file.readvAsync(0));
    }

    @Test
    void concurrentWrites() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        int numOps = 100;
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        List<ByteBuf> buffers = new ArrayList<>();

        for (int i = 0; i < numOps; i++) {
            byte[] data = String.format("%04d", i).getBytes(StandardCharsets.UTF_8);
            ByteBuf buf = Buffers.direct(data);
            buffers.add(buf);
            futures.add(file.writeAsync(buf, i * 4));
        }

        for (CompletableFuture<Integer> f : futures) {
            assertEquals(4, f.get());
        }

        for (ByteBuf buf : buffers) {
            buf.release();
        }
    }

    @Test
    void concurrentReads() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = new byte[400];
        for (int i = 0; i < 100; i++) {
            String s = String.format("%04d", i);
            System.arraycopy(s.getBytes(), 0, data, i * 4, 4);
        }
        ByteBuf writeBuf = Buffers.direct(data);
        file.writeAsync(writeBuf, 0).get();

        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        List<ByteBuf> buffers = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            ByteBuf buf = Buffers.direct(4);
            buffers.add(buf);
            futures.add(file.readAsync(buf, i * 4));
        }

        for (int i = 0; i < 100; i++) {
            assertEquals(4, futures.get(i).get());
            byte[] result = new byte[4];
            buffers.get(i).readBytes(result);
            assertEquals(String.format("%04d", i), new String(result));
        }

        writeBuf.release();
        for (ByteBuf buf : buffers) {
            buf.release();
        }
    }

    @Test
    void multipleCloseCallsSucceed() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        file.closeAsync().get();
        file.closeAsync().get();
        file.closeAsync().get();
    }

    @Test
    void readPastEndOfFileReturnsZero() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "short".getBytes(StandardCharsets.UTF_8);
        ByteBuf writeBuf = Buffers.direct(data);
        file.writeAsync(writeBuf, 0).get();

        ByteBuf readBuf = Buffers.direct(100);
        int read = file.readAsync(readBuf, 1000).get();
        assertEquals(0, read);

        writeBuf.release();
        readBuf.release();
    }

    @Test
    void largeWrite() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        int size = 1024 * 1024;
        byte[] data = new byte[size];
        Arrays.fill(data, (byte) 'X');
        ByteBuf writeBuf = Buffers.direct(data);

        int written = file.writeAsync(writeBuf, 0).get();
        assertEquals(size, written);

        FileStats stats = file.stat().get();
        assertEquals(size, stats.size);

        writeBuf.release();
    }

    @Test
    void punchHoleCreatesHole() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        int size = 8192;
        byte[] data = new byte[size];
        Arrays.fill(data, (byte) 'X');
        ByteBuf writeBuf = Buffers.direct(data);
        file.writeAsync(writeBuf, 0).get();

        file.punchHole(4096, 4096).get();
        file.fsync().get();

        FileStats stats = file.stat().get();
        assertEquals(size, stats.size);

        writeBuf.release();
    }

    @Test
    void bufferIndicesUpdatedOnRead() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);
        ByteBuf writeBuf = Buffers.direct(data);
        file.writeAsync(writeBuf, 0).get();

        ByteBuf readBuf = Buffers.direct(8);
        assertEquals(0, readBuf.writerIndex());

        file.readAsync(readBuf, 0).get();
        assertEquals(8, readBuf.writerIndex());

        writeBuf.release();
        readBuf.release();
    }

    @Test
    void bufferIndicesUpdatedOnWrite() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);
        ByteBuf writeBuf = Buffers.direct(data);
        assertEquals(0, writeBuf.readerIndex());
        assertEquals(8, writeBuf.readableBytes());

        file.writeAsync(writeBuf, 0).get();
        assertEquals(8, writeBuf.readerIndex());
        assertEquals(0, writeBuf.readableBytes());

        writeBuf.release();
    }

    @Test
    void openNonExistentFileWithoutCreateFails() {
        Path nonExistent = tempDir.resolve("does-not-exist-" + UUID.randomUUID());
        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            IoUringFile.open(nonExistent, eventLoop, READ).get());
        assertNotNull(ex.getCause());
    }

    @Test
    void openWithCreateNew() throws Exception {
        file = IoUringFile.open(testPath, eventLoop, READ, WRITE, CREATE_NEW).get();
        assertTrue(Files.exists(testPath));
    }

    @Test
    void openWithCreateNewOnExistingFails() throws Exception {
        Files.createFile(testPath);
        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            IoUringFile.open(testPath, eventLoop, READ, WRITE, CREATE_NEW).get());
        assertNotNull(ex.getCause());
    }

    @Test
    void truncateExistingOnOpen() throws Exception {
        Files.writeString(testPath, "existing content");
        assertTrue(Files.size(testPath) > 0);

        openFile(READ, WRITE, TRUNCATE_EXISTING);
        FileStats stats = file.stat().get();
        assertEquals(0, stats.size);
    }

    @Test
    void futureIsUncancellable() throws Exception {
        openFile(READ, WRITE, CREATE, TRUNCATE_EXISTING);

        byte[] data = "test".getBytes(StandardCharsets.UTF_8);
        ByteBuf buf = Buffers.direct(data);

        CompletableFuture<Integer> future = file.writeAsync(buf, 0);
        assertFalse(future.cancel(true));
        assertEquals(4, future.get());

        buf.release();
    }
}

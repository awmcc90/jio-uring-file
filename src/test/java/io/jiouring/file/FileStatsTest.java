package io.jiouring.file;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FileStatsTest {

    private ByteBuf createStatxBuffer() {
        return Buffers.direct(256, true);
    }

    @Test
    void parsesNlink() {
        ByteBuf buf = createStatxBuffer();
        buf.setInt(0x10, 42);
        FileStats stats = new FileStats(buf);
        assertEquals(42, stats.nlink);
        buf.release();
    }

    @Test
    void parsesUid() {
        ByteBuf buf = createStatxBuffer();
        buf.setInt(0x14, 1000);
        FileStats stats = new FileStats(buf);
        assertEquals(1000, stats.uid);
        buf.release();
    }

    @Test
    void parsesGid() {
        ByteBuf buf = createStatxBuffer();
        buf.setInt(0x18, 1001);
        FileStats stats = new FileStats(buf);
        assertEquals(1001, stats.gid);
        buf.release();
    }

    @Test
    void parsesMode() {
        ByteBuf buf = createStatxBuffer();
        buf.setShort(0x1C, (short) 420);
        FileStats stats = new FileStats(buf);
        assertEquals(420, stats.mode);
        buf.release();
    }

    @Test
    void parsesModeWithHighBitSet() {
        ByteBuf buf = createStatxBuffer();
        buf.setShort(0x1C, (short) 0xFFFF);
        FileStats stats = new FileStats(buf);
        assertEquals(0xFFFF, stats.mode);
        buf.release();
    }

    @Test
    void parsesSize() {
        ByteBuf buf = createStatxBuffer();
        buf.setLong(0x28, 1024L * 1024L * 1024L);
        FileStats stats = new FileStats(buf);
        assertEquals(1024L * 1024L * 1024L, stats.size);
        buf.release();
    }

    @Test
    void parsesAllFieldsTogether() {
        ByteBuf buf = createStatxBuffer();
        buf.setInt(0x10, 3);
        buf.setInt(0x14, 320);
        buf.setInt(0x18, 321);
        buf.setShort(0x1C, (short) 493);
        buf.setLong(0x28, 4096L);

        FileStats stats = new FileStats(buf);

        assertEquals(3, stats.nlink);
        assertEquals(320, stats.uid);
        assertEquals(321, stats.gid);
        assertEquals(493, stats.mode);
        assertEquals(4096L, stats.size);
        buf.release();
    }

    @Test
    void parsesZeroValues() {
        ByteBuf buf = createStatxBuffer();

        FileStats stats = new FileStats(buf);

        assertEquals(0, stats.nlink);
        assertEquals(0, stats.uid);
        assertEquals(0, stats.gid);
        assertEquals(0, stats.mode);
        assertEquals(0L, stats.size);
        buf.release();
    }

    @Test
    void parsesMaxIntValues() {
        ByteBuf buf = createStatxBuffer();
        buf.setInt(0x10, Integer.MAX_VALUE);
        buf.setInt(0x14, Integer.MAX_VALUE);
        buf.setInt(0x18, Integer.MAX_VALUE);

        FileStats stats = new FileStats(buf);

        assertEquals(Integer.MAX_VALUE, stats.nlink);
        assertEquals(Integer.MAX_VALUE, stats.uid);
        assertEquals(Integer.MAX_VALUE, stats.gid);
        buf.release();
    }

    @Test
    void parsesMaxLongSize() {
        ByteBuf buf = createStatxBuffer();
        buf.setLong(0x28, Long.MAX_VALUE);
        FileStats stats = new FileStats(buf);
        assertEquals(Long.MAX_VALUE, stats.size);
        buf.release();
    }

    @Test
    void parsesNegativeIntAsUnsigned() {
        ByteBuf buf = createStatxBuffer();
        buf.setInt(0x10, -1);
        FileStats stats = new FileStats(buf);
        assertEquals(-1, stats.nlink);
        buf.release();
    }

    @Test
    void modeMaskPreventSignExtension() {
        ByteBuf buf = createStatxBuffer();
        buf.setShort(0x1C, (short) 0x8000);
        FileStats stats = new FileStats(buf);
        assertEquals(0x8000, stats.mode);
        assertTrue(stats.mode > 0);
        buf.release();
    }

    @Test
    void regularFileMode() {
        ByteBuf buf = createStatxBuffer();
        int regularFile = 33188;
        buf.setShort(0x1C, (short) regularFile);
        FileStats stats = new FileStats(buf);
        assertEquals(regularFile & 0xFFFF, stats.mode);
        buf.release();
    }

    @Test
    void directoryMode() {
        ByteBuf buf = createStatxBuffer();
        int directory = 16877;
        buf.setShort(0x1C, (short) directory);
        FileStats stats = new FileStats(buf);
        assertEquals(directory & 0xFFFF, stats.mode);
        buf.release();
    }

    @Test
    void symbolicLinkMode() {
        ByteBuf buf = createStatxBuffer();
        int symlink = 41471;
        buf.setShort(0x1C, (short) symlink);
        FileStats stats = new FileStats(buf);
        assertEquals(symlink & 0xFFFF, stats.mode);
        buf.release();
    }
}

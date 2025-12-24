package io.jiouring.file;

import com.sun.nio.file.ExtendedOpenOption;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Set;

import static java.nio.file.StandardOpenOption.*;
import static java.nio.file.attribute.PosixFilePermission.*;
import static org.junit.jupiter.api.Assertions.*;

class OpenHelpersTest {

    @Test
    void cStrNullTerminates() {
        ByteBuf buf = OpenHelpers.cStr(Path.of("/tmp/test"));
        try {
            byte lastByte = buf.getByte(buf.readableBytes() - 1);
            assertEquals(0, lastByte);
        } finally {
            buf.release();
        }
    }

    @Test
    void cStrContainsAbsolutePath() {
        Path path = Path.of("/tmp/test");
        ByteBuf buf = OpenHelpers.cStr(path);
        try {
            byte[] bytes = new byte[buf.readableBytes() - 1];
            buf.getBytes(0, bytes);
            String str = new String(bytes, StandardCharsets.UTF_8);
            assertEquals(path.toAbsolutePath().toString(), str);
        } finally {
            buf.release();
        }
    }

    @Test
    void cStrHandlesUnicode() {
        Path path = Path.of("/tmp/日本語");
        ByteBuf buf = OpenHelpers.cStr(path);
        try {
            byte[] bytes = new byte[buf.readableBytes() - 1];
            buf.getBytes(0, bytes);
            String str = new String(bytes, StandardCharsets.UTF_8);
            assertTrue(str.endsWith("/tmp/日本語"));
        } finally {
            buf.release();
        }
    }

    @Test
    void cStrConvertsRelativeToAbsolute() {
        Path relative = Path.of("relative/path");
        ByteBuf buf = OpenHelpers.cStr(relative);
        try {
            byte[] bytes = new byte[buf.readableBytes() - 1];
            buf.getBytes(0, bytes);
            String str = new String(bytes, StandardCharsets.UTF_8);
            assertTrue(str.startsWith("/"));
            assertTrue(str.endsWith("relative/path"));
        } finally {
            buf.release();
        }
    }

    @Test
    void openFlagsDefaultsToReadOnly() {
        int flags = OpenHelpers.openFlags();
        assertEquals(NativeConstants.OpenFlags.RDONLY, flags);
    }

    @Test
    void openFlagsReadOnly() {
        int flags = OpenHelpers.openFlags(READ);
        assertEquals(NativeConstants.OpenFlags.RDONLY, flags);
    }

    @Test
    void openFlagsWriteOnly() {
        int flags = OpenHelpers.openFlags(WRITE);
        assertEquals(NativeConstants.OpenFlags.WRONLY, flags);
    }

    @Test
    void openFlagsReadWrite() {
        int flags = OpenHelpers.openFlags(READ, WRITE);
        assertEquals(NativeConstants.OpenFlags.RDWR, flags);
    }

    @Test
    void openFlagsWriteReadOrder() {
        int flags = OpenHelpers.openFlags(WRITE, READ);
        assertEquals(NativeConstants.OpenFlags.RDWR, flags);
    }

    @Test
    void openFlagsAppend() {
        int flags = OpenHelpers.openFlags(APPEND);
        assertTrue((flags & NativeConstants.OpenFlags.APPEND) != 0);
    }

    @Test
    void openFlagsTruncate() {
        int flags = OpenHelpers.openFlags(TRUNCATE_EXISTING);
        assertTrue((flags & NativeConstants.OpenFlags.TRUNC) != 0);
    }

    @Test
    void openFlagsCreate() {
        int flags = OpenHelpers.openFlags(CREATE);
        assertTrue((flags & NativeConstants.OpenFlags.CREAT) != 0);
    }

    @Test
    void openFlagsCreateNew() {
        int flags = OpenHelpers.openFlags(CREATE_NEW);
        assertTrue((flags & NativeConstants.OpenFlags.CREAT) != 0);
        assertTrue((flags & NativeConstants.OpenFlags.EXCL) != 0);
    }

    @Test
    void openFlagsSync() {
        int flags = OpenHelpers.openFlags(SYNC);
        assertTrue((flags & NativeConstants.OpenFlags.SYNC) != 0);
    }

    @Test
    void openFlagsDsync() {
        int flags = OpenHelpers.openFlags(DSYNC);
        assertTrue((flags & NativeConstants.OpenFlags.DSYNC) != 0);
    }

    @Test
    void openFlagsDirect() {
        int flags = OpenHelpers.openFlags(ExtendedOpenOption.DIRECT);
        assertTrue((flags & NativeConstants.OpenFlags.DIRECT) != 0);
    }

    @Test
    void openFlagsNofollow() {
        int flags = OpenHelpers.openFlags(LinkOption.NOFOLLOW_LINKS);
        assertTrue((flags & NativeConstants.OpenFlags.NOFOLLOW) != 0);
    }

    @Test
    void openFlagsCombined() {
        int flags = OpenHelpers.openFlags(READ, WRITE, CREATE, TRUNCATE_EXISTING);
        assertEquals(NativeConstants.OpenFlags.RDWR, flags & 0x3);
        assertTrue((flags & NativeConstants.OpenFlags.CREAT) != 0);
        assertTrue((flags & NativeConstants.OpenFlags.TRUNC) != 0);
    }

    @Test
    void openFlagsUnsupportedThrows() {
        OpenOption unsupported = new OpenOption() {};
        assertThrows(UnsupportedOperationException.class, () ->
            OpenHelpers.openFlags(unsupported));
    }

    @Test
    void openFlagsDeleteOnCloseThrows() {
        assertThrows(UnsupportedOperationException.class, () ->
            OpenHelpers.openFlags(DELETE_ON_CLOSE));
    }

    @Test
    void openFlagsSparseThrows() {
        assertThrows(UnsupportedOperationException.class, () ->
            OpenHelpers.openFlags(SPARSE));
    }

    @Test
    void fileModeDefaultWithNoAttributes() {
        int mode = OpenHelpers.fileMode();
        assertEquals(NativeConstants.FileMode.DEFAULT_FILE, mode);
    }

    @Test
    void fileModeOwnerRead() {
        Set<PosixFilePermission> perms = EnumSet.of(OWNER_READ);
        FileAttribute<?> attr = PosixFilePermissions.asFileAttribute(perms);
        int mode = OpenHelpers.fileMode(attr);
        assertEquals(NativeConstants.FileMode.S_IRUSR, mode);
    }

    @Test
    void fileModeOwnerWrite() {
        Set<PosixFilePermission> perms = EnumSet.of(OWNER_WRITE);
        FileAttribute<?> attr = PosixFilePermissions.asFileAttribute(perms);
        int mode = OpenHelpers.fileMode(attr);
        assertEquals(NativeConstants.FileMode.S_IWUSR, mode);
    }

    @Test
    void fileModeOwnerExecute() {
        Set<PosixFilePermission> perms = EnumSet.of(OWNER_EXECUTE);
        FileAttribute<?> attr = PosixFilePermissions.asFileAttribute(perms);
        int mode = OpenHelpers.fileMode(attr);
        assertEquals(NativeConstants.FileMode.S_IXUSR, mode);
    }

    @Test
    void fileModeGroupRead() {
        Set<PosixFilePermission> perms = EnumSet.of(GROUP_READ);
        FileAttribute<?> attr = PosixFilePermissions.asFileAttribute(perms);
        int mode = OpenHelpers.fileMode(attr);
        assertEquals(NativeConstants.FileMode.S_IRGRP, mode);
    }

    @Test
    void fileModeGroupWrite() {
        Set<PosixFilePermission> perms = EnumSet.of(GROUP_WRITE);
        FileAttribute<?> attr = PosixFilePermissions.asFileAttribute(perms);
        int mode = OpenHelpers.fileMode(attr);
        assertEquals(NativeConstants.FileMode.S_IWGRP, mode);
    }

    @Test
    void fileModeGroupExecute() {
        Set<PosixFilePermission> perms = EnumSet.of(GROUP_EXECUTE);
        FileAttribute<?> attr = PosixFilePermissions.asFileAttribute(perms);
        int mode = OpenHelpers.fileMode(attr);
        assertEquals(NativeConstants.FileMode.S_IXGRP, mode);
    }

    @Test
    void fileModeOthersRead() {
        Set<PosixFilePermission> perms = EnumSet.of(OTHERS_READ);
        FileAttribute<?> attr = PosixFilePermissions.asFileAttribute(perms);
        int mode = OpenHelpers.fileMode(attr);
        assertEquals(NativeConstants.FileMode.S_IROTH, mode);
    }

    @Test
    void fileModeOthersWrite() {
        Set<PosixFilePermission> perms = EnumSet.of(OTHERS_WRITE);
        FileAttribute<?> attr = PosixFilePermissions.asFileAttribute(perms);
        int mode = OpenHelpers.fileMode(attr);
        assertEquals(NativeConstants.FileMode.S_IWOTH, mode);
    }

    @Test
    void fileModeOthersExecute() {
        Set<PosixFilePermission> perms = EnumSet.of(OTHERS_EXECUTE);
        FileAttribute<?> attr = PosixFilePermissions.asFileAttribute(perms);
        int mode = OpenHelpers.fileMode(attr);
        assertEquals(NativeConstants.FileMode.S_IXOTH, mode);
    }

    @Test
    void fileMode644() {
        Set<PosixFilePermission> perms = EnumSet.of(
            OWNER_READ, OWNER_WRITE,
            GROUP_READ,
            OTHERS_READ
        );
        FileAttribute<?> attr = PosixFilePermissions.asFileAttribute(perms);
        int mode = OpenHelpers.fileMode(attr);
        assertEquals(0644, mode);
    }

    @Test
    void fileMode755() {
        Set<PosixFilePermission> perms = EnumSet.of(
            OWNER_READ, OWNER_WRITE, OWNER_EXECUTE,
            GROUP_READ, GROUP_EXECUTE,
            OTHERS_READ, OTHERS_EXECUTE
        );
        FileAttribute<?> attr = PosixFilePermissions.asFileAttribute(perms);
        int mode = OpenHelpers.fileMode(attr);
        assertEquals(0755, mode);
    }

    @Test
    void fileMode777() {
        Set<PosixFilePermission> perms = EnumSet.allOf(PosixFilePermission.class);
        FileAttribute<?> attr = PosixFilePermissions.asFileAttribute(perms);
        int mode = OpenHelpers.fileMode(attr);
        assertEquals(0777, mode);
    }

    @Test
    void fileModeEmptyPermissions() {
        Set<PosixFilePermission> perms = EnumSet.noneOf(PosixFilePermission.class);
        FileAttribute<?> attr = PosixFilePermissions.asFileAttribute(perms);
        int mode = OpenHelpers.fileMode(attr);
        assertEquals(0, mode);
    }

    @Test
    void fileModeIgnoresNonPosixAttributes() {
        FileAttribute<?> other = new FileAttribute<String>() {
            @Override public String name() { return "other:attribute"; }
            @Override public String value() { return "value"; }
        };
        int mode = OpenHelpers.fileMode(other);
        assertEquals(NativeConstants.FileMode.DEFAULT_FILE, mode);
    }

    @Test
    void fileModeLastPosixAttributeWins() {
        Set<PosixFilePermission> perms1 = EnumSet.of(OWNER_READ);
        Set<PosixFilePermission> perms2 = EnumSet.of(OWNER_WRITE);
        FileAttribute<?> attr1 = PosixFilePermissions.asFileAttribute(perms1);
        FileAttribute<?> attr2 = PosixFilePermissions.asFileAttribute(perms2);
        int mode = OpenHelpers.fileMode(attr1, attr2);
        assertEquals(NativeConstants.FileMode.S_IWUSR, mode);
    }
}

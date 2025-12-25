package io.jiouring.file;

import io.netty.buffer.ByteBuf;

public final class FileStats {

    public final int mask;
    public final int blksize;
    public final long attributes;
    public final int nlink;
    public final int uid;
    public final int gid;
    public final int mode;
    public final long inode;
    public final long size;
    public final long blocks;
    public final long attributesMask;

    public final Timestamp atime;
    public final Timestamp btime;
    public final Timestamp ctime;
    public final Timestamp mtime;

    public final int rdevMajor;
    public final int rdevMinor;
    public final int devMajor;
    public final int devMinor;

    public final long mntId;

    public final int dioMemAlign;
    public final int dioOffsetAlign;

    public final long subvol;

    public final int atomicWriteUnitMin;
    public final int atomicWriteUnitMax;
    public final int atomicWriteSegmentsMax;
    public final int dioReadOffsetAlign;
    public final int atomicWriteUnitMaxOpt;

    public static final class Timestamp {
        public final long seconds;
        public final int nanos;

        Timestamp(long seconds, int nanos) {
            this.seconds = seconds;
            this.nanos = nanos;
        }

        private static Timestamp readFrom(ByteBuf buf, int offset) {
            long sec = buf.getLong(offset);
            int nsec = buf.getInt(offset + 8);
            return new Timestamp(sec, nsec);
        }
    }

    public FileStats(ByteBuf buf) {
        mask = buf.getInt(0x00);
        blksize = buf.getInt(0x04);
        attributes = buf.getLong(0x08);
        nlink = buf.getInt(0x10);
        uid = buf.getInt(0x14);
        gid = buf.getInt(0x18);
        mode = buf.getUnsignedShort(0x1C);
        inode = buf.getLong(0x20);
        size = buf.getLong(0x28);
        blocks = buf.getLong(0x30);
        attributesMask = buf.getLong(0x38);

        atime = Timestamp.readFrom(buf, 0x40);
        btime = Timestamp.readFrom(buf, 0x50);
        ctime = Timestamp.readFrom(buf, 0x60);
        mtime = Timestamp.readFrom(buf, 0x70);

        rdevMajor = buf.getInt(0x80);
        rdevMinor = buf.getInt(0x84);
        devMajor = buf.getInt(0x88);
        devMinor = buf.getInt(0x8C);

        mntId = buf.getLong(0x90);

        dioMemAlign = buf.getInt(0x98);
        dioOffsetAlign = buf.getInt(0x9C);

        subvol = buf.getLong(0xA0);

        atomicWriteUnitMin = buf.getInt(0xA8);
        atomicWriteUnitMax = buf.getInt(0xAC);
        atomicWriteSegmentsMax = buf.getInt(0xB0);
        dioReadOffsetAlign = buf.getInt(0xB4);
        atomicWriteUnitMaxOpt = buf.getInt(0xB8);
    }
}

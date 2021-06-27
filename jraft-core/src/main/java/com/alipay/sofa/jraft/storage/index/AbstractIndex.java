package com.alipay.sofa.jraft.storage.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author hzh
 */
public abstract class AbstractIndex {

    private Logger LOG = LoggerFactory.getLogger(AbstractIndex.class);

    // Length of one index
    private int                     entrySize = 8 ;

    // File size
    private Long length;

    // File path
    private final String             path;


    private final File               file;

    // mmap byte buffer.
    private MappedByteBuffer         buffer;


    private volatile int             entries = 0;

    private  int                     maxEntries;



    // Wrote position.
    private volatile int             wrotePos;



    private final ReadWriteLock readWriteLock           = new ReentrantReadWriteLock(false);
    private final Lock          writeLock               = this.readWriteLock.writeLock();
    private final Lock          readLock                = this.readWriteLock.readLock();



    public AbstractIndex(final String path,final int maxSize) {
        this.path = path;
        this.file = new File(path);
        try {
            boolean newlyCreated = file.createNewFile();
            try(RandomAccessFile raf = new RandomAccessFile(path, "rw")) {
                if (newlyCreated) {
                    raf.setLength(maxSize);
                }
                this.length = raf.length();
                this.buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, this.length);
                if (newlyCreated) {
                    this.buffer.position(0);
                } else {
                    // If this file is existed , set position to last index entry
                    this.buffer.position(roundDownToExactMultiple(this.buffer.limit(), this.entrySize));
                }
                this.entries = this.buffer.position() / entrySize;
                this.maxEntries = this.buffer.limit() / entrySize;
            }
        } catch (final Throwable t) {
            LOG.error("Fail to init index file {}.", this.path, t);
        }
    }


    /**
     * Round a number to the greatest exact multiple of the given factor less than the given number.
     */
    public int roundDownToExactMultiple(final int number ,final int factor) {
        return factor * (number / factor);
    }


    /**
     * Flush data to disk
     */
    public void flush() {
        this.writeLock.lock();
        try {
            this.buffer.force();
        } finally {
            this.writeLock.unlock();
        }
    }


    


}

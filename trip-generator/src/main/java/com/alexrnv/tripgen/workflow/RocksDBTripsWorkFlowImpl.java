package com.alexrnv.tripgen.workflow;

import com.alexrnv.tripgen.dto.DataObjects;
import com.google.protobuf.ByteString;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created: 2/4/16 3:19 PM
 * Author: alex
 */
public class RocksDBTripsWorkFlowImpl extends TripsWorkFlow {

    static {
        RocksDB.loadLibrary();
    }

    private final RocksDB db;
    private final Options options;
    //private boolean useMerge = true;
    private boolean plainTable = false;

    public RocksDBTripsWorkFlowImpl(String dbPath)  {

        ///clean db for debugging
        //Arrays.stream(Paths.get(dbPath).toFile().listFiles())
        //        .forEach(f -> f.delete());
        ///

        this.options = new Options()
                .useFixedLengthPrefixExtractor(10)
                .setCreateIfMissing(true)
                .setMaxOpenFiles(-1)
                .setMaxBackgroundCompactions(4)
                .setWriteBufferSize(512 * SizeUnit.MB)
                .setMaxWriteBufferNumber(3)
                .setCompressionPerLevel(
                        Arrays.asList(CompressionType.NO_COMPRESSION,
                                CompressionType.NO_COMPRESSION,
                                CompressionType.SNAPPY_COMPRESSION,
                                CompressionType.SNAPPY_COMPRESSION,
                                CompressionType.SNAPPY_COMPRESSION,
                                CompressionType.SNAPPY_COMPRESSION,
                                CompressionType.ZLIB_COMPRESSION))
                .setCompactionStyle(CompactionStyle.UNIVERSAL)
                .setMemtablePrefixBloomBits(16)
                .setIncreaseParallelism(4)
                .createStatistics()
                .setBloomLocality(16)
                .setMemTableConfig(new HashSkipListMemTableConfig())

        ;

        if(plainTable) {
            TableFormatConfig tableFormatConfig = new PlainTableConfig()
                    .setBloomBitsPerKey(10);
            options.setTableFormatConfig(tableFormatConfig) //for mmap only
                    .setAllowMmapReads(true)
                    .setAllowMmapWrites(true)
                    ;
        } else {
            Filter bloomFilter = new BloomFilter(16);
            BlockBasedTableConfig table_options = new BlockBasedTableConfig();
            table_options
                    .setIndexType(IndexType.kHashSearch)
                    .setBlockCacheSize(4 * SizeUnit.KB)
                    .setFilter(bloomFilter)
                    .setCacheNumShardBits(16)
                    .setBlockSizeDeviation(5)
                    .setBlockRestartInterval(10)
                            //.setCacheIndexAndFilterBlocks(true)
                    .setHashIndexAllowCollision(false)
                    .setBlockCacheCompressedSize(64 * SizeUnit.KB)
                    .setBlockCacheCompressedNumShardBits(16);
            options.setTableFormatConfig(table_options);
        }

        this.options.getEnv()
                .setBackgroundThreads(32, Env.FLUSH_POOL)
                .setBackgroundThreads(32, Env.COMPACTION_POOL);
        try {
            this.db = RocksDB.open(options, dbPath);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Started workflow");
    }

    @Override
    public void processProbe(DataObjects.Probe probe) throws IOException {
        super.processProbe(probe);
    }

    @Override
    protected void completePrevTripAndCreateNew(DataObjects.Trip prev, DataObjects.Probe probe) throws IOException {
        DataObjects.Trip updated = completeTrip(prev);
        byte[] bytes = updated.getTripId().toByteArray();
        WriteBatch writeBatch = new WriteBatch();
        writeBatch.put(bytes, updated.toByteArray());
        DataObjects.Trip newTrip = newTripFromProbe(probe);
        writeBatch.put(bytes, newTrip.toByteArray());
        try {
            db.write(new WriteOptions(), writeBatch);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void createNewTrip(DataObjects.Probe probe) throws IOException {
        updateTrip(newTripFromProbe(probe));
    }

    @Override
    protected DataObjects.Trip findPendingTrip(ByteString sensorId) throws IOException {
        try {
            //tripId == sensorId
            byte[] bytes = db.get(sensorId.toByteArray());
            return bytes != null ? DataObjects.Trip.parseFrom(bytes) : null;
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void updateTrip(DataObjects.Trip trip) throws IOException {
        try {
            db.put(trip.getTripId().toByteArray(), trip.toByteArray());
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (db != null)
            db.close();
        options.dispose();
        super.finalize();
    }

}

package com.flair.caching.flaircaching.repositories;

import com.flair.caching.flaircaching.dto.CacheCountEntry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionPriority;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

@Repository
@Slf4j
@RequiredArgsConstructor
public class CacheRepository {

    static {
        RocksDB.loadLibrary();
    }

    private RocksDB rocksDB;

//    cf_options.level_compaction_dynamic_level_bytes = true;
//    options.max_background_compactions = 4;
//    options.max_background_flushes = 2;
//    options.bytes_per_sync = 1048576;
//    options.compaction_pri = kMinOverlappingRatio;
//    table_options.block_size = 16 * 1024;
//    table_options.cache_index_and_filter_blocks = true;
//    table_options.pin_l0_filter_and_index_blocks_in_cache = true;

    @PostConstruct
    public void init() throws RocksDBException {
        log.info("Initializing rocksdb repo");
        try {
            this.rocksDB = RocksDB.open(getOptions(), "cache");
        } catch (RocksDBException e) {
            log.error("Error opening rocks db", e);
            throw e;
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down rocksdb repo");
        try {
            this.rocksDB.flush(new FlushOptions());
        } catch (RocksDBException e) {
            log.warn("Failed to flush db before cleanup", e);
        } finally {
            this.rocksDB.close();
        }
    }

    public CacheEntryResult getResult(String table, String key) {
        try {
            byte[] cacheEntryKey = getCacheEntryKey(table, key);
            byte[] cacheCountEntryKey = getCacheCountEntryKey(table, key);

            Map<byte[], byte[]> cacheValues = this.rocksDB.multiGet(Arrays.asList(cacheEntryKey, cacheCountEntryKey));
            byte[] cacheEntryValue = cacheValues.get(cacheEntryKey);
            byte[] cacheCountEntryValue = cacheValues.get(cacheCountEntryKey);

            return new CacheEntryResult()
                    .setCacheEntry(Optional.ofNullable(cacheEntryValue)
                            .map(it -> (CacheEntry) SerializationUtils.deserialize(cacheEntryValue))
                            .orElse(null))
                    .setCacheCountEntry(Optional.ofNullable(cacheCountEntryValue)
                            .map(it2 -> (CacheCountEntry) SerializationUtils.deserialize(it2))
                            .orElse(null));
        } catch (RocksDBException e) {
            throw new RuntimeException("Rocksdb error", e);
        }
    }

    private ColumnFamilyOptions createCfOptions() {
        return new ColumnFamilyOptions()
                .setCompactionPriority(CompactionPriority.MinOverlappingRatio)
                .optimizeUniversalStyleCompaction()
                .setLevelCompactionDynamicLevelBytes(true);
    }

    private DBOptions getDbOptions() {
        return new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxBackgroundCompactions(4)
                .setMaxBackgroundFlushes(2)
                .setBytesPerSync(1048576);
    }

    private Options getOptions() {
        return new Options()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxBackgroundCompactions(4)
                .setMaxBackgroundFlushes(2)
                .setBytesPerSync(1048576)
                .setCompactionPriority(CompactionPriority.MinOverlappingRatio);
    }

    public void putResult(String table, String key, String value,
                          Long refreshAfterDate,
                          Long purgeAfterDate,
                          Integer refreshAfterCount,
                          Long dateCreated,
                          CacheCountEntry cacheCountEntry) {
        try (WriteBatch writeBatch = new WriteBatch()) {
            byte[] cacheKey = getCacheEntryKey(table, key);

            CacheEntry cacheEntry = new CacheEntry()
                    .setResult(value)
                    .setKey(key)
                    .setDateCreated(dateCreated)
                    .setPurgeAfterDate(purgeAfterDate)
                    .setRefreshAfterDate(refreshAfterDate)
                    .setRefreshAfterCount(refreshAfterCount);

            writeBatch.put(cacheKey, SerializationUtils.serialize(cacheEntry));

            if (cacheCountEntry != null) {
                byte[] countEntryKey = getCacheCountEntryKey(table, key);
                writeBatch.put(countEntryKey, SerializationUtils.serialize(cacheCountEntry));
            }

            this.rocksDB.write(new WriteOptions(), writeBatch);
        } catch (RocksDBException e) {
            throw new RuntimeException("Rocksdb error", e);
        }
    }

    private byte[] getCacheEntryKey(String table, String key) {
        return ("_cache." + table + "." + key).getBytes();
    }

    private byte[] getCacheCountEntryKey(String table, String key) {
        return ("_count." + table + "." + key).getBytes();
    }

    private Collection<CacheEntry> listKeys(ColumnFamilyHandle columnFamily) {
        Collection<CacheEntry> keys = new ArrayList<>();
        RocksIterator itr = this.rocksDB.newIterator(columnFamily);
        itr.seekToFirst();
        while (itr.isValid()) {
            keys.add((CacheEntry) SerializationUtils.deserialize(itr.key()));
            itr.next();
        }
        return keys;
    }

    public void putCount(String key, String table, CacheCountEntry cacheCountEntry) {
        byte[] cacheCountEntryKey = getCacheCountEntryKey(table, key);
        try {
            this.rocksDB.put(cacheCountEntryKey, SerializationUtils.serialize(cacheCountEntry));
        } catch (RocksDBException e) {
            throw new RuntimeException("Rocksdb error", e);
        }
    }

    public Optional<CacheCountEntry> getCount(String key, String table) {
        byte[] cacheCountEntryKey = getCacheCountEntryKey(table, key);
        try {
            return Optional.ofNullable(this.rocksDB.get(cacheCountEntryKey))
                    .map(it -> (CacheCountEntry)SerializationUtils.deserialize(it));
        } catch (RocksDBException e) {
            throw new RuntimeException("Rocksdb error", e);
        }
    }
}

package com.flair.caching.flaircaching.repositories;

import com.flair.caching.flaircaching.dto.CacheCountEntry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.rocksdb.ColumnFamilyDescriptor;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

@Repository
@Slf4j
@RequiredArgsConstructor
public class CacheRepository {

    public static final String COL_ENTRIES = "entries";
    public static final String COL_COUNTS = "counts";

    static {
        RocksDB.loadLibrary();
    }

    private RocksDB rocksDB;
    private final Map<String, ColumnFamilyHandle> columnFamilies = new ConcurrentHashMap<>();
    private final List<ColumnFamilyHandle> cfHandlesList = new CopyOnWriteArrayList<>();
    private ColumnFamilyOptions cfOptions;
    private DBOptions dbOptions;

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

        cfOptions = createCfOptions();
        dbOptions = getDbOptions();

        try {
            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions),
                    new ColumnFamilyDescriptor(COL_ENTRIES.getBytes(), cfOptions),
                    new ColumnFamilyDescriptor(COL_COUNTS.getBytes(), cfOptions)
            );

            this.rocksDB = RocksDB.open(dbOptions, "cache", cfDescriptors, cfHandlesList);
            this.columnFamilies.put(COL_ENTRIES, cfHandlesList.get(1));
            this.columnFamilies.put(COL_COUNTS, cfHandlesList.get(2));
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
        }
        this.cfHandlesList.forEach(it -> it.close());
        this.cfHandlesList.clear();
        this.columnFamilies.clear();
        this.dbOptions.close();
        this.rocksDB.close();
        this.cfOptions.close();
    }

    public CacheEntryResult getResult(String table, String key) {
        try {
            String cacheEntryKey = getCacheEntryKey(table, key);
            String cacheCountEntryKey = getCacheCountEntryKey(table, key);

            byte[] cacheEntryKeyBytes = cacheEntryKey.getBytes();
            byte[] countEntryKeyBytes = cacheCountEntryKey.getBytes();
            Map<byte[], byte[]> cacheValues = this.rocksDB.multiGet(
                    Arrays.asList(this.columnFamilies.get(COL_ENTRIES), this.columnFamilies.get(COL_COUNTS)),
                    Arrays.asList(cacheEntryKeyBytes, countEntryKeyBytes)
            );
            byte[] cacheEntryValue = cacheValues.get(cacheEntryKeyBytes);
            byte[] cacheCountEntryValue = cacheValues.get(countEntryKeyBytes);

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
            String cacheKey = getCacheEntryKey(table, key);

            CacheEntry cacheEntry = new CacheEntry()
                    .setResult(value)
                    .setKey(cacheKey)
                    .setDateCreated(dateCreated)
                    .setPurgeAfterDate(purgeAfterDate)
                    .setRefreshAfterDate(refreshAfterDate)
                    .setRefreshAfterCount(refreshAfterCount);

            writeBatch.put(this.columnFamilies.get(COL_ENTRIES), cacheKey.getBytes(), SerializationUtils.serialize(cacheEntry));

            if (cacheCountEntry != null) {
                String countEntryKey = getCacheCountEntryKey(table, key);
                writeBatch.put(this.columnFamilies.get(COL_COUNTS), countEntryKey.getBytes(), SerializationUtils.serialize(cacheCountEntry));
            }

            this.rocksDB.write(new WriteOptions(), writeBatch);
        } catch (RocksDBException e) {
            throw new RuntimeException("Rocksdb error", e);
        }
    }

    private String getCacheEntryKey(String table, String key) {
        return (table + "." + key);
    }

    private String getCacheCountEntryKey(String table, String key) {
        return (table + "." + key);
    }

    private Collection<String> listKeys() {
        Collection<String> keys = new ArrayList<>();
        RocksIterator itr = this.rocksDB.newIterator(this.columnFamilies.get(COL_ENTRIES));
        itr.seekToFirst();
        while (itr.isValid()) {
            keys.add(new String(itr.key()));
            itr.next();
        }
        itr.close();
        return keys;
    }

    public void putCount(String key, String table, CacheCountEntry cacheCountEntry) {
        String cacheCountEntryKey = getCacheCountEntryKey(table, key);
        try {
            this.rocksDB.put(this.columnFamilies.get(COL_COUNTS), cacheCountEntryKey.getBytes(), SerializationUtils.serialize(cacheCountEntry));
        } catch (RocksDBException e) {
            throw new RuntimeException("Rocksdb error", e);
        }
    }

    public Optional<CacheCountEntry> getCount(String key, String table) {
        String cacheCountEntryKey = getCacheCountEntryKey(table, key);
        try {
            return Optional.ofNullable(this.rocksDB.get(this.columnFamilies.get(COL_COUNTS), cacheCountEntryKey.getBytes()))
                    .map(it -> (CacheCountEntry) SerializationUtils.deserialize(it));
        } catch (RocksDBException e) {
            throw new RuntimeException("Rocksdb error", e);
        }
    }

    public List<CacheEntry> list() {
        final Collection<String> keys = listKeys();
        if (keys.size() == 0) {
            return Collections.emptyList();
        }

        final List<ColumnFamilyHandle> columnFamilyHandles = Collections.nCopies(keys.size(),
                this.columnFamilies.get(COL_ENTRIES));
        final List<byte[]> keyBytes = keys.stream()
                .map(String::getBytes)
                .collect(Collectors.toList());
        final Map<byte[], byte[]> map;
        try {
            map = this.rocksDB.multiGet(columnFamilyHandles, keyBytes);
        } catch (RocksDBException e) {
            throw new RuntimeException("Rocksdb error", e);
        }
        return map.values()
                .stream()
                .map(it -> (CacheEntry) SerializationUtils.deserialize(it))
                .collect(Collectors.toList());
    }

    public void delete(Collection<CacheEntry> entries) {
        final ColumnFamilyHandle cfEntry = this.columnFamilies.get(COL_ENTRIES);
        final ColumnFamilyHandle cfCount = this.columnFamilies.get(COL_COUNTS);
        entries.forEach(it -> {
            try {
                final byte[] keyBytes = it.getKey().getBytes();
                this.rocksDB.delete(cfEntry, keyBytes);
                this.rocksDB.delete(cfCount, keyBytes);
            } catch (RocksDBException e) {
                throw new RuntimeException("Rocksdb error", e);
            }
        });
    }
}

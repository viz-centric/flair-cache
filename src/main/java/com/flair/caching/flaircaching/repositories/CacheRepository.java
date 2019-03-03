package com.flair.caching.flaircaching.repositories;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionPriority;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
@Slf4j
@RequiredArgsConstructor
public class CacheRepository {

    static {
        RocksDB.loadLibrary();
    }

//    cf_options.level_compaction_dynamic_level_bytes = true;
//    options.max_background_compactions = 4;
//    options.max_background_flushes = 2;
//    options.bytes_per_sync = 1048576;
//    options.compaction_pri = kMinOverlappingRatio;
//    table_options.block_size = 16 * 1024;
//    table_options.cache_index_and_filter_blocks = true;
//    table_options.pin_l0_filter_and_index_blocks_in_cache = true;

    public Optional<String> getResult(String table, String key) {
        try {
            byte[] totalKey = getTotalKey(table, key);
            try (
                    final Options options = getOptions();
                    final RocksDB db = RocksDB.open(options, "cache")
            ) {
                return Optional.ofNullable(db.get(totalKey))
                        .map(it -> new String(it));
            }
        } catch (RocksDBException e) {
            throw new RuntimeException("Rocksdb error", e);
        }
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

    private ColumnFamilyOptions createCfOptions() {
        return new ColumnFamilyOptions()
                .optimizeUniversalStyleCompaction()
                .setLevelCompactionDynamicLevelBytes(true);
    }

    public void putResult(String table, String key, String value) {
        try {
            byte[] totalKey = getTotalKey(table, key);
            try (
                    final Options options = getOptions();
                    final RocksDB db = RocksDB.open(options,
                            "cache")
            ) {
                db.put(totalKey, value.getBytes());
            }
        } catch (RocksDBException e) {
            throw new RuntimeException("Rocksdb error", e);
        }
    }

    private byte[] getTotalKey(String table, String key) {
        return (table + "." + key).getBytes();
    }

}

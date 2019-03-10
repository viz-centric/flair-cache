package com.flair.caching.flaircaching.repositories;

import com.flair.caching.flaircaching.dto.CacheEntry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.CompactionPriority;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Repository;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

@Repository
@Slf4j
@RequiredArgsConstructor
public class CacheRepository {

    static {
        RocksDB.loadLibrary();
    }

    private final Clock clock;

//    cf_options.level_compaction_dynamic_level_bytes = true;
//    options.max_background_compactions = 4;
//    options.max_background_flushes = 2;
//    options.bytes_per_sync = 1048576;
//    options.compaction_pri = kMinOverlappingRatio;
//    table_options.block_size = 16 * 1024;
//    table_options.cache_index_and_filter_blocks = true;
//    table_options.pin_l0_filter_and_index_blocks_in_cache = true;

    public Optional<CacheEntry> getResult(String table, String key) {
        try {
            byte[] cacheKey = getTotalKey(table, key);
            byte[] cacheValue;
            try (
                    final Options options = getOptions();
                    final RocksDB db = RocksDB.open(options, "cache")
            ) {
                cacheValue = db.get(cacheKey);
            }

            if (cacheValue == null) {
                return Optional.empty();
            }

            com.flair.bi.messages.CacheEntry cacheEntry;
            try (ByteArrayInputStream input = new ByteArrayInputStream(cacheValue)) {
                cacheEntry = com.flair.bi.messages.CacheEntry.parseDelimitedFrom(input);
            }

            return Optional.ofNullable(cacheEntry)
                    .map(it -> new CacheEntry()
                            .setResult(it.getValue())
                            .setDateCreated(Instant.ofEpochSecond(it.getDateCreated())));

        } catch (IOException | RocksDBException e) {
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

    public void putResult(String table, String key, String value) {
        try {
            byte[] totalKey = getTotalKey(table, key);
            try (
                    final Options options = getOptions();
                    final RocksDB db = RocksDB.open(options, "cache")
            ) {
                db.put(totalKey, value.getBytes());
            }
        } catch (RocksDBException e) {
            throw new RuntimeException("Rocksdb error", e);
        }
    }

    public void putResult(String table, String key, String value,
                          Long refreshAfterDate,
                          Long purgeAfterDate,
                          Integer refreshAfterCount) {
        try {
            byte[] cacheKey = getTotalKey(table, key);
            long epochSecond = Instant.now(clock).getEpochSecond();
            com.flair.bi.messages.CacheEntry cacheEntry = com.flair.bi.messages.CacheEntry.newBuilder()
                    .setValue(value)
                    .setKey(key)
                    .setDateCreated(epochSecond)
                    .setPurgeAfterDate(purgeAfterDate != null ? purgeAfterDate : epochSecond)
                    .setRefreshAfterDate(refreshAfterDate != null ? refreshAfterDate : epochSecond)
                    .setRefreshAfterCount(refreshAfterCount != null ? refreshAfterCount : 0)
                    .build();

            byte[] cacheValue;
            try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                cacheEntry.writeDelimitedTo(output);
                cacheValue = output.toByteArray();
            }

            try (
                    final Options options = getOptions();
                    final RocksDB db = RocksDB.open(options, "cache")
            ) {
                db.put(cacheKey, cacheValue);
            }
        } catch (RocksDBException | IOException e) {
            throw new RuntimeException("Rocksdb error", e);
        }
    }

    private byte[] getTotalKey(String table, String key) {
        return (table + "." + key).getBytes();
    }

}

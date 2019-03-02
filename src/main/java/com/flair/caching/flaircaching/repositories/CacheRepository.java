package com.flair.caching.flaircaching.repositories;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyOptions;
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

    public Optional<String> getResult(String table, String key) {
        try {
            try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {

                byte[] totalKey = getTotalKey(table, key);

                try (
                        final Options options = new Options()
                                .setCreateIfMissing(true)
                                .setCreateMissingColumnFamilies(true);
                        final RocksDB db = RocksDB.open(options,
                                "cache")
                ) {
                    return Optional.ofNullable(db.get(totalKey))
                            .map(it -> new String(it));
                }
            }
        } catch (RocksDBException e) {
            throw new RuntimeException("Rocksdb error", e);
        }
    }

    public void putResult(String table, String key, String value) {
        try {
            try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {

                byte[] totalKey = getTotalKey(table, key);

                try (
                        final Options options = new Options()
                                .setCreateIfMissing(true)
                                .setCreateMissingColumnFamilies(true);
                        final RocksDB db = RocksDB.open(options,
                                "cache")
                ) {
                    db.put(totalKey, value.getBytes());
                }
            }
        } catch (RocksDBException e) {
            throw new RuntimeException("Rocksdb error", e);
        }
    }

    private byte[] getTotalKey(String table, String key) {
        return (table + "." + key).getBytes();
    }

}

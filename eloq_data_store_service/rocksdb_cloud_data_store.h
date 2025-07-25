/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */
#pragma once

#include <rocksdb/db.h>

#include <memory>
#include <string>

#include "data_store.h"
#include "data_store_service.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb_config.h"
#include "rocksdb_data_store_common.h"

namespace EloqDS
{

class RocksDBCloudDataStore : public RocksDBDataStoreCommon
{
public:
    RocksDBCloudDataStore(const EloqDS::RocksDBCloudConfig &cloud_config,
                          const EloqDS::RocksDBConfig &config,
                          bool create_if_missing,
                          bool tx_enable_cache_replacement,
                          uint32_t shard_id,
                          DataStoreService *data_store_service);

    ~RocksDBCloudDataStore();

    /**
     * @brief Build S3 client factory.
     *        Used to connect to S3 compatible storage, e.g. MinIO.
     * @param endpoint The endpoint of S3.
     * @return The S3 client factory.
     */
    rocksdb::S3ClientFactory BuildS3ClientFactory(const std::string &endpoint);

    /**
     * @brief Open the cloud database.
     * @param cfs_options The cloud file system options.
     * @return True if open successfully, otherwise false.
     */
    bool StartDB(std::string cookie = "",
                 std::string prev_cookie = "") override;

    /**
     * @brief Close the cloud database.
     */
    void Shutdown() override;

protected:
    /**
     * @brief Get the RocksDB pointer.
     */
    rocksdb::DBCloud *GetDBPtr() override;

private:
    /// Helper functions for cloud manifest files and cookies
    inline std::string MakeCloudManifestCookie(int64_t cc_ng_id, int64_t term);
    inline std::string MakeCloudManifestFile(const std::string &dbname,
                                             int64_t cc_ng_id,
                                             int64_t term);
    inline bool IsCloudManifestFile(const std::string &filename);
    inline std::vector<std::string> SplitString(const std::string &str,
                                                char delimiter);
    inline bool GetCookieFromCloudManifestFile(const std::string &filename,
                                               int64_t &cc_ng_id,
                                               int64_t &term);
    inline int64_t FindMaxTermFromCloudManifestFiles(
        const std::shared_ptr<ROCKSDB_NAMESPACE::CloudStorageProvider>
            &storage_provider,
        const std::string &bucket_prefix,
        const std::string &bucket_name,
        const int64_t cc_ng_id_in_cookie);

    /* Convert a string into a long long. Returns 1 if the string could be
     * parsed into a (non-overflowing) long long, 0 otherwise. The value will be
     * set to the parsed value when appropriate.
     *
     * Note that this function demands that the string strictly represents
     * a long long: no spaces or other characters before or after the string
     * representing the number are accepted, nor zeroes at the start if not
     * for the string "0" representing the zero number.
     *
     * Because of its strictness, it is safe to use this function to check if
     * you can convert a string into a long long, and obtain back the string
     * from the number without any loss in the string representation. */
    bool String2ll(const char *s, size_t slen, int64_t &value);

    /**
     * @brief Open the cloud database.
     * @param cfs_options The cloud file system options.
     * @return True if open successfully, otherwise false.
     */
    bool OpenCloudDB(const rocksdb::CloudFileSystemOptions &cfs_options);

private:
    const EloqDS::RocksDBCloudConfig cloud_config_;
    rocksdb::CloudFileSystemOptions cfs_options_;
    std::shared_ptr<rocksdb::FileSystem> cloud_fs_{nullptr};
    std::unique_ptr<rocksdb::Env> cloud_env_{nullptr};
    rocksdb::DBCloud *db_{nullptr};
};

}  // namespace EloqDS
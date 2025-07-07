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
#include "rocksdb_cloud_data_store.h"

#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <bthread/condition_variable.h>
#include <rocksdb/db.h>
#include <rocksdb/listener.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/statistics.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "data_store_service.h"
#include "ds_request.pb.h"
#include "internal_request.h"
#include "rocksdb/cloud/cloud_storage_provider.h"

#define LONG_STR_SIZE 21

namespace EloqDS
{

/* Convert a string into a long long. Returns 1 if the string could be parsed
 * into a (non-overflowing) long long, 0 otherwise. The value will be set to
 * the parsed value when appropriate.
 *
 * Note that this function demands that the string strictly represents
 * a long long: no spaces or other characters before or after the string
 * representing the number are accepted, nor zeroes at the start if not
 * for the string "0" representing the zero number.
 *
 * Because of its strictness, it is safe to use this function to check if
 * you can convert a string into a long long, and obtain back the string
 * from the number without any loss in the string representation. */
static inline bool string2ll(const char *s, size_t slen, int64_t &value)
{
    const char *p = s;
    size_t plen = 0;
    int negative = 0;
    uint64_t v;

    /* A string of zero length or excessive length is not a valid number. */
    if (plen == slen || slen >= LONG_STR_SIZE)
        return 0;

    /* Special case: first and only digit is 0. */
    if (slen == 1 && p[0] == '0')
    {
        value = 0;
        return 1;
    }

    /* Handle negative numbers: just set a flag and continue like if it
     * was a positive number. Later convert into negative. */
    if (p[0] == '-')
    {
        negative = 1;
        p++;
        plen++;

        /* Abort on only a negative sign. */
        if (plen == slen)
            return 0;
    }

    /* First digit should be 1-9, otherwise the string should just be 0. */
    if (p[0] >= '1' && p[0] <= '9')
    {
        v = p[0] - '0';
        p++;
        plen++;
    }
    else
    {
        return 0;
    }

    /* Parse all the other digits, checking for overflow at every step. */
    while (plen < slen && p[0] >= '0' && p[0] <= '9')
    {
        if (v > (ULLONG_MAX / 10)) /* Overflow. */
            return 0;
        v *= 10;

        if (v > (ULLONG_MAX - (p[0] - '0'))) /* Overflow. */
            return 0;
        v += p[0] - '0';

        p++;
        plen++;
    }

    /* Return if not all bytes were used. */
    if (plen < slen)
        return 0;

    /* Convert to negative if needed, and do the final overflow check when
     * converting from unsigned long long to long long. */
    if (negative)
    {
        if (v > ((unsigned long long) (-(LLONG_MIN + 1)) + 1)) /* Overflow. */
            return 0;
        value = -v;
    }
    else
    {
        if (v > LLONG_MAX) /* Overflow. */
            return 0;

        value = v;
    }
    return 1;
}

// Build key in RocksDB
static const std::string BuildKey(const std::string_view table_name,
                                  uint32_t partition_id,
                                  const std::string_view key)
{
    std::string tmp_key;
    tmp_key.reserve(table_name.size() + 2 + key.size());
    tmp_key.append(table_name);
    tmp_key.append(KEY_SEPARATOR);
    tmp_key.append(std::to_string(partition_id));
    tmp_key.append(KEY_SEPARATOR);
    tmp_key.append(key);
    return tmp_key;
}

[[maybe_unused]] static const std::string BuildKeyForDebug(
    const std::unique_ptr<rocksdb::Slice[]> &key_slices, size_t slice_size)
{
    std::string tmp_key;
    for (size_t i = 0; i < slice_size; ++i)
    {
        tmp_key.append(key_slices[i].data(), key_slices[i].size());
    }
    return tmp_key;
}

static const std::string BuildKeyPrefix(const std::string_view table_name,
                                        uint32_t partition_id)
{
    std::string tmp_key;
    tmp_key.reserve(table_name.size() + 1);
    tmp_key.append(table_name);
    tmp_key.append(KEY_SEPARATOR);
    tmp_key.append(std::to_string(partition_id));
    tmp_key.append(KEY_SEPARATOR);
    return tmp_key;
}

static void BuildKey(const std::string_view prefix,
                     const std::string_view key,
                     std::string &key_out)
{
    key_out.append(prefix);
    // if prefix ends with KEY_SEPARATOR, skiping append another KEY_SEPARATOR
    if (prefix.back() != KEY_SEPARATOR[0])
    {
        key_out.append(KEY_SEPARATOR);
    }
    key_out.append(key);
}

static void BuildKeyPrefixSlices(
    const std::string_view table_name,
    const std::string_view partition_id,
    std::unique_ptr<rocksdb::Slice[]> &key_slices_out)
{
    key_slices_out[0] = rocksdb::Slice(table_name.data(), table_name.size());
    key_slices_out[1] = rocksdb::Slice(KEY_SEPARATOR, 1);
    key_slices_out[2] =
        rocksdb::Slice(partition_id.data(), partition_id.size());
    key_slices_out[3] = rocksdb::Slice(KEY_SEPARATOR, 1);
}

static void BuildKeySlices(std::unique_ptr<rocksdb::Slice[]> &key_slices_out,
                           const WriteRecordsRequest *batch_write_req,
                           const size_t &idx,
                           const uint16_t parts_cnt_per_key)
{
    for (uint16_t i = 0; i < parts_cnt_per_key; ++i)
    {
        uint16_t key_slice_idx = 4 + i;
        size_t key_part_idx = idx * parts_cnt_per_key + i;
        key_slices_out[key_slice_idx] =
            rocksdb::Slice(batch_write_req->GetKeyPart(key_part_idx).data(),
                           batch_write_req->GetKeyPart(key_part_idx).size());
    }
}

inline bool DeserializeKey(const char *data,
                           const size_t size,
                           const std::string_view table_name,
                           const uint32_t partition_id,
                           std::string_view &key_out)
{
    size_t prefix_size =
        table_name.size() + 1 + std::to_string(partition_id).size() + 1;
    if (prefix_size >= size)
    {
        return false;
    }

    key_out = std::string_view(data + prefix_size, size - prefix_size);
    return true;
}

inline void EncodeHasTTLIntoTs(uint64_t &ts, bool has_ttl)
{
    // Set the MSB to indicate that the timestamp is encoded
    if (has_ttl)
    {
        ts |= MSB;
    }
    else
    {
        // ts remains unchanged, since its MSB is always be 0
        // ts &= MSB_MASK;  // Clear the MSB
    }
}

static uint16_t TransformRecordToValueSlices(
    const WriteRecordsRequest *batch_write_req,
    const size_t &idx,
    const uint16_t parts_cnt_per_record,
    uint64_t &ts,
    const uint64_t &ttl,
    std::unique_ptr<rocksdb::Slice[]> &value_slices)
{
    bool has_ttl = (ttl > 0);
    EncodeHasTTLIntoTs(ts, has_ttl);
    value_slices[0] =
        rocksdb::Slice(reinterpret_cast<const char *>(&ts), sizeof(uint64_t));
    size_t value_slice_idx = 1;
    if (has_ttl)
    {
        value_slices[1] = rocksdb::Slice(reinterpret_cast<const char *>(&ttl),
                                         sizeof(uint64_t));
        value_slice_idx++;
    }

    for (uint16_t i = 0; i < parts_cnt_per_record; ++i)
    {
        size_t record_slice_idx = value_slice_idx + i;
        size_t record_part_idx = idx * parts_cnt_per_record + i;
        value_slices[record_slice_idx] = rocksdb::Slice(
            batch_write_req->GetRecordPart(record_part_idx).data(),
            batch_write_req->GetRecordPart(record_part_idx).size());
    }
    return value_slice_idx + parts_cnt_per_record;
}

static void DecodeHasTTLFromTs(uint64_t &ts, bool &has_ttl)
{
    // Check if the MSB is set
    if (ts & MSB)
    {
        has_ttl = true;
        ts &= MSB_MASK;  // Clear the MSB
    }
    else
    {
        has_ttl = false;
    }
}

static void DeserializeValueToRecord(const char *data,
                                     const size_t size,
                                     std::string &record,
                                     uint64_t &ts,
                                     uint64_t &ttl)
{
    assert(size >= sizeof(uint64_t));
    size_t offset = 0;
    ts = *reinterpret_cast<const uint64_t *>(data);
    offset += sizeof(uint64_t);
    bool has_ttl = false;
    DecodeHasTTLFromTs(ts, has_ttl);
    if (has_ttl)
    {
        assert(size >= sizeof(uint64_t) * 2);
        ttl = *(reinterpret_cast<const uint64_t *>(data + offset));
        offset += sizeof(uint64_t);
    }
    else
    {
        assert(size >= sizeof(uint64_t));
        ttl = 0;
    }
    record.assign(data + offset, size - offset);
}

RocksDBCloudDataStore::RocksDBCloudDataStore(
    const EloqDS::RocksDBCloudConfig &cloud_config,
    const EloqDS::RocksDBConfig &config,
    bool create_if_missing,
    bool tx_enable_cache_replacement,
    uint32_t shard_id,
    DataStoreService *data_store_service)
    : DataStore(shard_id, data_store_service),
      enable_stats_(config.enable_stats_),
      stats_dump_period_sec_(config.stats_dump_period_sec_),
      storage_path_(config.storage_path_ + "/ds_" + std::to_string(shard_id)),
      max_write_buffer_number_(config.max_write_buffer_number_),
      max_background_jobs_(config.max_background_jobs_),
      max_background_flushes_(config.max_background_flush_),
      max_background_compactions_(config.max_background_compaction_),
      target_file_size_base_(config.target_file_size_base_bytes_),
      target_file_size_multiplier_(config.target_file_size_multiplier_),
      write_buff_size_(config.write_buffer_size_bytes_),
      use_direct_io_for_flush_and_compaction_(
          config.use_direct_io_for_flush_and_compaction_),
      use_direct_io_for_read_(config.use_direct_io_for_read_),
      level0_stop_writes_trigger_(config.level0_stop_writes_trigger_),
      level0_slowdown_writes_trigger_(config.level0_slowdown_writes_trigger_),
      level0_file_num_compaction_trigger_(
          config.level0_file_num_compaction_trigger_),
      max_bytes_for_level_base_(config.max_bytes_for_level_base_bytes_),
      max_bytes_for_level_multiplier_(config.max_bytes_for_level_multiplier_),
      compaction_style_(config.compaction_style_),
      soft_pending_compaction_bytes_limit_(
          config.soft_pending_compaction_bytes_limit_bytes_),
      hard_pending_compaction_bytes_limit_(
          config.hard_pending_compaction_bytes_limit_bytes_),
      max_subcompactions_(config.max_subcompactions_),
      write_rate_limit_(config.write_rate_limit_bytes_),
      batch_write_size_(config.batch_write_size_),
      periodic_compaction_seconds_(config.periodic_compaction_seconds_),
      dialy_offpeak_time_utc_(config.dialy_offpeak_time_utc_),
      db_path_(storage_path_ + "/db/"),
      ckpt_path_(storage_path_ + "/rocksdb_snapshot/"),
      backup_path_(storage_path_ + "/backups/"),
      received_snapshot_path_(storage_path_ + "/received_snapshot/"),
      create_db_if_missing_(create_if_missing),
      tx_enable_cache_replacement_(tx_enable_cache_replacement),
      ttl_compaction_filter_(nullptr),
      cloud_config_(cloud_config),
      cloud_fs_(),
      cloud_env_(nullptr),
      db_(nullptr)
{
    info_log_level_ = StringToInfoLogLevel(config.info_log_level_);
    query_worker_pool_ =
        std::make_unique<ThreadWorkerPool>(config.query_worker_num_);
}

RocksDBCloudDataStore::~RocksDBCloudDataStore()
{
    if (query_worker_pool_ != nullptr || data_store_service_ != nullptr ||
        db_ != nullptr)
    {
        Shutdown();
    }
}

void RocksDBCloudDataStore::Shutdown()
{
    std::unique_lock<std::shared_mutex> db_lk(db_mux_);

    // shutdown query worker pool
    query_worker_pool_->Shutdown();
    query_worker_pool_ = nullptr;

    data_store_service_->ForceEraseScanIters(shard_id_);
    data_store_service_ = nullptr;

    if (db_ != nullptr)
    {
        DLOG(INFO) << "RocksDBCloudDataStore Shutdown, db->Close()";
        db_->Close();
        DLOG(INFO) << "RocksDBCloudDataStore Shutdown, delete db_";
        delete db_;
        DLOG(INFO) << "RocksDBCloudDataStore Shutdown, db_ = nullptr";
        db_ = nullptr;
        DLOG(INFO) << "RocksDBCloudDataStore Shutdown, ttl_compaction_filter_ "
                      "= nullptr";
        ttl_compaction_filter_ = nullptr;
        DLOG(INFO) << "RocksDBCloudDataStore Shutdown, cloud_env_ = nullptr";
        cloud_env_ = nullptr;
        DLOG(INFO) << "RocksDBCloudDataStore Shutdown, cloud_fs_ = nullptr";
        cloud_fs_ = nullptr;
    }
}

std::string toLower(const std::string &str)
{
    std::string lowerStr = str;
    std::transform(lowerStr.begin(),
                   lowerStr.end(),
                   lowerStr.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return lowerStr;
}

rocksdb::S3ClientFactory RocksDBCloudDataStore::BuildS3ClientFactory(
    const std::string &endpoint)
{
    return [endpoint](const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>
                          &credentialsProvider,
                      const Aws::Client::ClientConfiguration &baseConfig)
               -> std::shared_ptr<Aws::S3::S3Client>
    {
        // Check endpoint url start with http or https
        if (endpoint.empty())
        {
            return nullptr;
        }

        std::string endpoint_url = toLower(endpoint);

        bool secured_url = false;
        if (endpoint_url.rfind("http://", 0) == 0)
        {
            secured_url = false;
        }
        else if (endpoint_url.rfind("https://", 0) == 0)
        {
            secured_url = true;
        }
        else
        {
            LOG(ERROR) << "Invalid S3 endpoint url";
            std::abort();
        }

        // Create a new configuration based on the base config
        Aws::Client::ClientConfiguration config = baseConfig;
        config.endpointOverride = endpoint_url;
        if (secured_url)
        {
            config.scheme = Aws::Http::Scheme::HTTPS;
        }
        else
        {
            config.scheme = Aws::Http::Scheme::HTTP;
        }
        // Disable SSL verification for HTTPS
        config.verifySSL = false;

        // Create and return the S3 client
        if (credentialsProvider)
        {
            return std::make_shared<Aws::S3::S3Client>(
                credentialsProvider,
                config,
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                true /* useVirtualAddressing */);
        }
        else
        {
            return std::make_shared<Aws::S3::S3Client>(config);
        }
    };
}

bool RocksDBCloudDataStore::StartDB(std::string cookie, std::string prev_cookie)
{
    if (db_)
    {
        // db is already started, no op
        DLOG(INFO) << "DBCloud already started";
        return true;
    }

#ifdef DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3
    if (cloud_config_.aws_access_key_id_.length() == 0 ||
        cloud_config_.aws_secret_key_.length() == 0)
    {
        LOG(INFO) << "No AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                     "provided, use default credential provider";
        cfs_options_.credentials.type = rocksdb::AwsAccessType::kUndefined;
    }
    else
    {
        cfs_options_.credentials.InitializeSimple(
            cloud_config_.aws_access_key_id_, cloud_config_.aws_secret_key_);
    }

    rocksdb::Status status = cfs_options_.credentials.HasValid();
    if (!status.ok())
    {
        LOG(ERROR) << "Valid AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                      "is required, error: "
                   << status.ToString();
        return false;
    }
#endif

    cfs_options_.src_bucket.SetBucketName(cloud_config_.bucket_name_,
                                          cloud_config_.bucket_prefix_);
    cfs_options_.src_bucket.SetRegion(cloud_config_.region_);
    cfs_options_.src_bucket.SetObjectPath("rocksdb_cloud");
    cfs_options_.dest_bucket.SetBucketName(cloud_config_.bucket_name_,
                                           cloud_config_.bucket_prefix_);
    cfs_options_.dest_bucket.SetRegion(cloud_config_.region_);
    cfs_options_.dest_bucket.SetObjectPath("rocksdb_cloud");
    // Add sst_file_cache for accerlating random access on sst files
    cfs_options_.sst_file_cache =
        rocksdb::NewLRUCache(cloud_config_.sst_file_cache_size_);
    // delay cloud file deletion for 1 hour
    cfs_options_.cloud_file_deletion_delay =
        std::chrono::seconds(cloud_config_.db_file_deletion_delay_);

    // keep invisible files in cloud storage since they can be referenced
    // by other nodes with old valid cloud manifest files during leader transfer
    cfs_options_.delete_cloud_invisible_files_on_open = false;

    // sync cloudmanifest and manifest files when open db
    cfs_options_.resync_on_open = true;

    // use aws transfer manager to upload/download files
    // the transfer manager can leverage multipart upload and download
    cfs_options_.use_aws_transfer_manager = true;

    if (!cloud_config_.s3_endpoint_url_.empty())
    {
        cfs_options_.s3_client_factory =
            BuildS3ClientFactory(cloud_config_.s3_endpoint_url_);
    }

    DLOG(INFO) << "DBCloud Open";
    rocksdb::CloudFileSystem *cfs;
    // Open the cloud file system
    status = EloqDS::NewCloudFileSystem(cfs_options_, &cfs);
    if (!status.ok())
    {
        LOG(ERROR) << "Unable to create cloud storage filesystem, cloud type: "
#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
                   << "Aws"
#elif defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS)
                   << "Gcp"
#endif
                   << ", at path rocksdb_cloud with bucket "
                   << cfs_options_.src_bucket.GetBucketName()
                   << ", with error: " << status.ToString();

        return false;
    }

    std::string cookie_on_open = "";
    std::string new_cookie_on_open = "";

    // TODO(githubzilla): ng_id is not used in the current implementation,
    // remove it later
    int64_t ng_id = 0;
    auto storage_provider = cfs->GetStorageProvider();
    int64_t max_term =
        FindMaxTermFromCloudManifestFiles(storage_provider,
                                          cloud_config_.bucket_prefix_,
                                          cloud_config_.bucket_name_,
                                          ng_id);

    if (max_term != -1)
    {
        cookie_on_open = MakeCloudManifestCookie(ng_id, max_term);
        new_cookie_on_open = MakeCloudManifestCookie(ng_id, max_term + 1);
    }
    else
    {
        cookie_on_open = "";
        new_cookie_on_open = MakeCloudManifestCookie(ng_id, 0);
    }

    // new CLOUDMANIFEST suffixed by cookie and epochID suffixed
    // MANIFEST files are generated, which won't overwrite the old ones
    // opened by previous leader
    auto &cfs_options_ref = cfs->GetMutableCloudFileSystemOptions();
    cfs_options_.cookie_on_open = cookie_on_open;
    cfs_options_ref.cookie_on_open = cookie_on_open;
    cfs_options_.new_cookie_on_open = new_cookie_on_open;
    cfs_options_ref.new_cookie_on_open = new_cookie_on_open;

    DLOG(INFO) << "StartDB cookie_on_open: " << cfs_options_.cookie_on_open
               << " new_cookie_on_open: " << cfs_options_.new_cookie_on_open;

    cloud_fs_.reset(cfs);
    // Create options and use the AWS file system that we created
    // earlier
    cloud_env_ = rocksdb::NewCompositeEnv(cloud_fs_);

    return OpenCloudDB(cfs_options_);
}

bool RocksDBCloudDataStore::OpenCloudDB(
    const rocksdb::CloudFileSystemOptions &cfs_options)
{
    rocksdb::Options options;
    options.env = cloud_env_.get();
    options.create_if_missing = create_db_if_missing_;
    options.create_missing_column_families = true;
    // boost write performance by enabling unordered write
    options.unordered_write = true;

    // print db statistics every 60 seconds
    if (enable_stats_)
    {
        options.statistics = rocksdb::CreateDBStatistics();
        options.stats_dump_period_sec = stats_dump_period_sec_;
    }

    // Max background jobs number, rocksdb will auto turn max flush(1/4 of
    // max_background_jobs) and compaction jobs(3/4 of max_background_jobs)
    if (max_background_jobs_ > 0)
    {
        options.max_background_jobs = max_background_jobs_;
    }

    if (max_background_flushes_ > 0)
    {
        options.max_background_flushes = max_background_flushes_;
    }

    if (max_background_compactions_ > 0)
    {
        options.max_background_compactions = max_background_compactions_;
    }

    options.use_direct_io_for_flush_and_compaction =
        use_direct_io_for_flush_and_compaction_;
    options.use_direct_reads = use_direct_io_for_read_;

    // Set compation style
    if (compaction_style_ == "universal")
    {
        LOG(WARNING)
            << "Universal compaction has a size limitation. Please be careful "
               "when your DB (or column family) size is over 100GB";
        options.compaction_style = rocksdb::kCompactionStyleUniversal;
    }
    else if (compaction_style_ == "level")
    {
        options.compaction_style = rocksdb::kCompactionStyleLevel;
    }
    else if (compaction_style_ == "fifo")
    {
        LOG(ERROR) << "FIFO compaction style should not be used";
        std::abort();
    }
    else
    {
        LOG(ERROR) << "Invalid compaction style: " << compaction_style_;
        std::abort();
    }

    // set the max subcompactions
    if (max_subcompactions_ > 0)
    {
        options.max_subcompactions = max_subcompactions_;
    }

    // set the write rate limit
    if (write_rate_limit_ > 0)
    {
        options.rate_limiter.reset(
            rocksdb::NewGenericRateLimiter(write_rate_limit_));
    }

    options.info_log_level = info_log_level_;
    options.best_efforts_recovery = false;
    options.skip_checking_sst_file_sizes_on_db_open = true;
    options.skip_stats_update_on_db_open = true;
    // Important! keep atomic_flush true, since we disabled WAL
    options.atomic_flush = true;
    auto db_event_listener = std::make_shared<RocksDBEventListener>();
    options.listeners.emplace_back(db_event_listener);

    // The following two configuration items are setup for purpose of removing
    // expired kv data items according to their ttl Rocksdb will compact all sst
    // files which are older than periodic_compaction_seconds_ at
    // dialy_offpeak_time_utc_ Then all kv data items in the sst files will go
    // through the TTLCompactionFilter which is configurated for column family
    options.periodic_compaction_seconds = periodic_compaction_seconds_;
    options.daily_offpeak_time_utc = dialy_offpeak_time_utc_;

    // The max_open_files default value is -1, it cause DB open all files on
    // DB::Open() This behavior causes 2 effects,
    // 1. DB::Open() will be slow
    // 2. During DB::Open, some of the opened sst files keep in LRUCache
    // will be deleted due to LRU policy, which causes DB::Open failed
    options.max_open_files = 0;

    if (target_file_size_base_ > 0)
    {
        options.target_file_size_base = target_file_size_base_;
    }

    if (target_file_size_multiplier_ > 0)
    {
        options.target_file_size_multiplier = target_file_size_multiplier_;
    }

    // mem table size
    if (write_buff_size_ > 0)
    {
        options.write_buffer_size = write_buff_size_;
    }
    // Max write buffer number
    if (max_write_buffer_number_ > 0)
    {
        options.max_write_buffer_number = max_write_buffer_number_;
    }

    if (level0_slowdown_writes_trigger_ > 0)
    {
        options.level0_slowdown_writes_trigger =
            level0_slowdown_writes_trigger_;
    }

    if (level0_stop_writes_trigger_ > 0)
    {
        options.level0_stop_writes_trigger = level0_stop_writes_trigger_;
    }

    if (level0_file_num_compaction_trigger_ > 0)
    {
        options.level0_file_num_compaction_trigger =
            level0_file_num_compaction_trigger_;
    }

    if (soft_pending_compaction_bytes_limit_ > 0)
    {
        options.soft_pending_compaction_bytes_limit =
            soft_pending_compaction_bytes_limit_;
    }

    if (hard_pending_compaction_bytes_limit_ > 0)
    {
        options.hard_pending_compaction_bytes_limit =
            hard_pending_compaction_bytes_limit_;
    }

    if (max_bytes_for_level_base_ > 0)
    {
        options.max_bytes_for_level_base = max_bytes_for_level_base_;
    }

    if (max_bytes_for_level_multiplier_ > 0)
    {
        options.max_bytes_for_level_multiplier =
            max_bytes_for_level_multiplier_;
    }

    // set ttl compaction filter
    assert(ttl_compaction_filter_ == nullptr);
    ttl_compaction_filter_ = std::make_unique<EloqDS::TTLCompactionFilter>();

    options.compaction_filter =
        static_cast<rocksdb::CompactionFilter *>(ttl_compaction_filter_.get());

    auto start = std::chrono::system_clock::now();
    std::unique_lock<std::shared_mutex> db_lk(db_mux_);
    auto status = rocksdb::DBCloud::Open(options, db_path_, "", 0, &db_);

    auto end = std::chrono::system_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    DLOG(INFO) << "DBCloud Open took " << duration.count() << " ms";

    if (!status.ok())
    {
        ttl_compaction_filter_ = nullptr;

        LOG(ERROR) << "Unable to open db at path " << storage_path_
                   << " with bucket " << cfs_options.src_bucket.GetBucketName()
                   << " with error: " << status.ToString();

        // db does not exist. This node cannot escalate to be the ng leader.
        return false;
    }

    if (cloud_config_.warm_up_thread_num_ != 0)
    {
        db_->WarmUp(cloud_config_.warm_up_thread_num_);
    }

    // Reset max_open_files to default value of -1 after DB::Open
    db_->SetDBOptions({{"max_open_files", "-1"}});

    LOG(INFO) << "RocksDB Cloud started";
    return true;
}

rocksdb::DBCloud *RocksDBCloudDataStore::GetDBPtr()
{
    return db_;
}

rocksdb::InfoLogLevel RocksDBCloudDataStore::StringToInfoLogLevel(
    const std::string &log_level_str)
{
    if (log_level_str == "DEBUG")
    {
        return rocksdb::InfoLogLevel::DEBUG_LEVEL;
    }
    else if (log_level_str == "INFO")
    {
        return rocksdb::InfoLogLevel::INFO_LEVEL;
    }
    else if (log_level_str == "WARN")
    {
        return rocksdb::InfoLogLevel::WARN_LEVEL;
    }
    else if (log_level_str == "ERROR")
    {
        return rocksdb::InfoLogLevel::ERROR_LEVEL;
    }
    else
    {
        // If the log level string is not recognized, default to a specific log
        // level, e.g., INFO_LEVEL Alternatively, you could throw an exception
        // or handle the case as you see fit
        return rocksdb::InfoLogLevel::INFO_LEVEL;
    }
}

inline std::string RocksDBCloudDataStore::MakeCloudManifestCookie(
    int64_t cc_ng_id, int64_t term)
{
    return std::to_string(cc_ng_id) + "-" + std::to_string(term);
}

inline std::string RocksDBCloudDataStore::MakeCloudManifestFile(
    const std::string &dbname, int64_t cc_ng_id, int64_t term)
{
    if (cc_ng_id < 0 || term < 0)
    {
        return dbname + "/CLOUDMANIFEST";
    }

    assert(cc_ng_id >= 0 && term >= 0);

    return dbname + "/CLOUDMANIFEST-" + std::to_string(cc_ng_id) + "-" +
           std::to_string(term);
}

inline bool RocksDBCloudDataStore::IsCloudManifestFile(
    const std::string &filename)
{
    return filename.find("CLOUDMANIFEST") != std::string::npos;
}

// Helper function to split a string by a delimiter
inline std::vector<std::string> RocksDBCloudDataStore::SplitString(
    const std::string &str, char delimiter)
{
    std::vector<std::string> tokens;
    std::stringstream ss(str);
    std::string token;
    while (std::getline(ss, token, delimiter))
    {
        tokens.push_back(token);
    }
    return tokens;
}

inline bool RocksDBCloudDataStore::GetCookieFromCloudManifestFile(
    const std::string &filename, int64_t &cc_ng_id, int64_t &term)
{
    const std::string prefix = "CLOUDMANIFEST";
    auto pos = filename.rfind('/');
    std::string manifest_part =
        (pos != std::string::npos) ? filename.substr(pos + 1) : filename;

    // Check if the filename starts with "CLOUDMANIFEST"
    if (manifest_part.find(prefix) != 0)
    {
        return false;
    }

    // Remove the prefix "CLOUDMANIFEST" to parse the rest
    std::string suffix = manifest_part.substr(prefix.size());

    // If there's no suffix
    if (suffix.empty())
    {
        cc_ng_id = -1;
        term = -1;
        return false;
    }
    else if (suffix[0] == '-')
    {
        // Parse the "-cc_ng_id-term" format
        suffix = suffix.substr(1);  // Remove the leading '-'
        std::vector<std::string> parts = SplitString(suffix, '-');
        if (parts.size() != 2)
        {
            return false;
        }

        bool res = string2ll(parts[0].c_str(), parts[0].size(), cc_ng_id);
        if (!res)
        {
            return false;
        }
        res = string2ll(parts[1].c_str(), parts[1].size(), term);
        return res;
    }

    return false;
}

inline int64_t RocksDBCloudDataStore::FindMaxTermFromCloudManifestFiles(
    const std::shared_ptr<ROCKSDB_NAMESPACE::CloudStorageProvider>
        &storage_provider,
    const std::string &bucket_prefix,
    const std::string &bucket_name,
    const int64_t cc_ng_id_in_cookie)
{
    // find the max term cookie from cloud manifest files
    // read only db should be opened with the latest cookie
    auto start = std::chrono::system_clock::now();
    std::vector<std::string> cloud_objects;
    std::string cloud_manifest_prefix = "CLOUDMANIFEST-";
    cloud_manifest_prefix.append(std::to_string(cc_ng_id_in_cookie));
    auto st = storage_provider->ListCloudObjectsWithPrefix(
        bucket_prefix + bucket_name,
        "rocksdb_cloud",
        cloud_manifest_prefix,
        &cloud_objects);
    if (!st.ok())
    {
        LOG(ERROR) << "Failed to list cloud objects, error: " << st.ToString();
        return -1;
    }

    int64_t max_term = -1;
    for (const auto &object : cloud_objects)
    {
        LOG(INFO) << "FindMaxTermFromCloudManifestFiles, object: " << object;
        if (IsCloudManifestFile(object))
        {
            int64_t cc_ng_id;
            int64_t term;
            bool res = GetCookieFromCloudManifestFile(object, cc_ng_id, term);
            if (cc_ng_id_in_cookie == cc_ng_id && res)
            {
                if (term > max_term)
                {
                    max_term = term;
                }
            }
        }
    }
    LOG(INFO) << "FindMaxTermFromCloudManifestFiles, cc_ng_id: "
              << cc_ng_id_in_cookie << " max_term: " << max_term;
    auto end = std::chrono::system_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    LOG(INFO) << "FindMaxTermFromCloudManifestFiles tooks " << duration.count()
              << " ms";

    return max_term;
}

bool RocksDBCloudDataStore::Initialize()
{
    // before opening rocksdb, rocksdb_storage_path_ must exist, create it
    // if not exist
    std::error_code error_code;
    bool rocksdb_storage_path_exists =
        std::filesystem::exists(db_path_, error_code);
    if (error_code.value() != 0)
    {
        LOG(ERROR) << "unable to check rocksdb directory: " << db_path_
                   << ", error code: " << error_code.value()
                   << ", error message: " << error_code.message();
        return false;
    }
    if (!rocksdb_storage_path_exists)
    {
        std::filesystem::create_directories(db_path_, error_code);
        if (error_code.value() != 0)
        {
            LOG(ERROR) << "unable to create rocksdb directory: " << db_path_
                       << ", error code: " << error_code.value()
                       << ", error message: " << error_code.message();
            return false;
        }
    }

    // Cleanup previous snapshots
    rocksdb_storage_path_exists =
        std::filesystem::exists(ckpt_path_, error_code);
    if (error_code.value() != 0)
    {
        LOG(ERROR) << "unable to check rocksdb directory: " << ckpt_path_
                   << ", error code: " << error_code.value()
                   << ", error message: " << error_code.message();
        return false;
    }
    if (!rocksdb_storage_path_exists)
    {
        std::filesystem::create_directories(ckpt_path_, error_code);
        if (error_code.value() != 0)
        {
            LOG(ERROR) << "unable to create rocksdb directory: " << ckpt_path_
                       << ", error code: " << error_code.value()
                       << ", error message: " << error_code.message();
            return false;
        }
    }
    else
    {
        // clean up previous snapshots
        for (const auto &entry :
             std::filesystem::directory_iterator(ckpt_path_))
        {
            std::filesystem::remove_all(entry.path());
        }
    }

    rocksdb_storage_path_exists =
        std::filesystem::exists(received_snapshot_path_, error_code);
    if (error_code.value() != 0)
    {
        LOG(ERROR) << "unable to check rocksdb directory: "
                   << received_snapshot_path_
                   << ", error code: " << error_code.value()
                   << ", error message: " << error_code.message();
        return false;
    }
    if (!rocksdb_storage_path_exists)
    {
        std::filesystem::create_directories(received_snapshot_path_,
                                            error_code);
        if (error_code.value() != 0)
        {
            LOG(ERROR) << "unable to create rocksdb directory: "
                       << received_snapshot_path_
                       << ", error code: " << error_code.value()
                       << ", error message: " << error_code.message();
            return false;
        }
    }
    else
    {
        // clean up previous snapshots
        for (const auto &entry :
             std::filesystem::directory_iterator(received_snapshot_path_))
        {
            std::filesystem::remove_all(entry.path());
        }
    }

    rocksdb_storage_path_exists =
        std::filesystem::exists(backup_path_, error_code);
    if (error_code.value() != 0)
    {
        LOG(ERROR) << "unable to check rocksdb directory: " << backup_path_
                   << ", error code: " << error_code.value()
                   << ", error message: " << error_code.message();
        return false;
    }
    if (!rocksdb_storage_path_exists)
    {
        std::filesystem::create_directories(backup_path_, error_code);
        if (error_code.value() != 0)
        {
            LOG(ERROR) << "unable to create rocksdb directory: " << backup_path_
                       << ", error code: " << error_code.value()
                       << ", error message: " << error_code.message();
            return false;
        }
    }

    return true;
}

void RocksDBCloudDataStore::FlushData(FlushDataRequest *flush_data_req)
{
    query_worker_pool_->SubmitWork(
        [this, flush_data_req]()
        {
            // Create a guard to ensure the poolable object is released to pool
            std::unique_ptr<PoolableGuard> poolable_guard =
                std::make_unique<PoolableGuard>(flush_data_req);

            // Increase write counter at the start of the operation
            IncreaseWriteCounter();

            ::EloqDS::remote::CommonResult result;
            std::shared_lock<std::shared_mutex> db_lk(db_mux_);
            auto db = GetDBPtr();
            if (!db)
            {
                DecreaseWriteCounter();  // Decrease counter before error return
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
                result.set_error_msg("DB is not opened");
                flush_data_req->SetFinish(result);
                return;
            }

            rocksdb::FlushOptions flush_options;
            flush_options.allow_write_stall = true;
            flush_options.wait = true;

            auto status = db->Flush(flush_options);
            if (!status.ok())
            {
                LOG(ERROR) << "Unable to flush db with error: "
                           << status.ToString();
                DecreaseWriteCounter();  // Decrease counter before error return
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::FLUSH_FAILED);
                result.set_error_msg(status.ToString());
                flush_data_req->SetFinish(result);
                return;
            }

            // Decrease counter before successful return
            DecreaseWriteCounter();
            result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
            flush_data_req->SetFinish(result);
            DLOG(INFO) << "FlushData successfully.";
        });
}

void RocksDBCloudDataStore::DeleteRange(DeleteRangeRequest *delete_range_req)
{
    query_worker_pool_->SubmitWork(
        [this, delete_range_req]()
        {
            // Create a guard to ensure the poolable object is released to pool
            std::unique_ptr<PoolableGuard> poolable_guard =
                std::make_unique<PoolableGuard>(delete_range_req);

            // Increase write counter at the start of the operation
            IncreaseWriteCounter();

            ::EloqDS::remote::CommonResult result;
            std::shared_lock<std::shared_mutex> db_lk(db_mux_);
            auto db = GetDBPtr();
            if (!db)
            {
                // Decrease counter before error return
                DecreaseWriteCounter();
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
                result.set_error_msg("DB is not opened");
                delete_range_req->SetFinish(result);
                return;
            }

            const std::string_view table_name =
                delete_range_req->GetTableName();
            const uint32_t partition_id = delete_range_req->GetPartitionId();

            // build start key
            std::string start_key_str = BuildKey(
                table_name, partition_id, delete_range_req->GetStartKey());
            rocksdb::Slice start_key(start_key_str);

            // build end key
            std::string end_key_str;
            auto end_key_strv = delete_range_req->GetEndKey();
            if (end_key_strv.empty())
            {
                // If end_key is empty, delete all keys starting from start_key
                end_key_str = BuildKey(table_name, partition_id, "");
                end_key_str.back()++;
            }
            else
            {
                end_key_str = BuildKey(table_name, partition_id, end_key_strv);
            }
            rocksdb::Slice end_key(end_key_str);

            rocksdb::WriteOptions write_opts;
            write_opts.disableWAL = delete_range_req->SkipWal();
            auto status = db->DeleteRange(write_opts, start_key, end_key);
            if (!status.ok())
            {
                LOG(ERROR) << "Unable to delete range with error: "
                           << status.ToString();

                // Decrease counter before error return
                DecreaseWriteCounter();
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::WRITE_FAILED);
                result.set_error_msg(status.ToString());
                delete_range_req->SetFinish(result);
                return;
            }

            // Decrease counter before successful return
            DecreaseWriteCounter();
            result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
            delete_range_req->SetFinish(result);
            DLOG(INFO) << "DeleteRange successfully.";
        });
}

void RocksDBCloudDataStore::Read(ReadRequest *req)
{
    query_worker_pool_->SubmitWork(
        [this, req]()
        {
            // Create a guard to ensure the poolable object is released to pool
            std::unique_ptr<PoolableGuard> poolable_guard =
                std::make_unique<PoolableGuard>(req);

            auto table_name = req->GetTableName();
            uint32_t partition_id = req->GetPartitionId();
            auto key = req->GetKey();

            std::shared_lock<std::shared_mutex> db_lk(db_mux_);

            auto *db = GetDBPtr();
            if (db == nullptr)
            {
                req->SetFinish(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
                return;
            }

            std::string key_str = BuildKey(table_name, partition_id, key);
            std::string value;
            rocksdb::ReadOptions read_options;
            rocksdb::Status status = db->Get(read_options, key_str, &value);

            if (status.ok())
            {
                std::string rec;
                uint64_t rec_ts;
                uint64_t rec_ttl;
                DeserializeValueToRecord(
                    value.data(), value.size(), rec, rec_ts, rec_ttl);
                req->SetRecord(std::move(rec));
                req->SetRecordTs(rec_ts);
                req->SetRecordTtl(rec_ttl);
                req->SetFinish(::EloqDS::remote::DataStoreError::NO_ERROR);
            }
            else if (status.IsNotFound())
            {
                req->SetRecord("");
                req->SetRecordTs(0);
                req->SetRecordTtl(0);
                req->SetFinish(::EloqDS::remote::DataStoreError::KEY_NOT_FOUND);
            }
            else
            {
                req->SetFinish(::EloqDS::remote::DataStoreError::READ_FAILED);
                LOG(ERROR) << "RocksdbCloud read key:" << key_str
                           << " failed, status:" << status.ToString();
            }
        });
}

void RocksDBCloudDataStore::BatchWriteRecords(
    WriteRecordsRequest *batch_write_req)
{
    assert(batch_write_req != nullptr);
    if (batch_write_req->RecordsCount() == 0)
    {
        ::EloqDS::remote::CommonResult result;
        result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
        batch_write_req->SetFinish(result);
        return;
    }

    query_worker_pool_->SubmitWork(
        [this, batch_write_req]() mutable
        {
            // Create a guard to ensure the poolable object is released to pool
            std::unique_ptr<PoolableGuard> poolable_guard =
                std::make_unique<PoolableGuard>(batch_write_req);

            auto shard_status = FetchDSShardStatus();
            ::EloqDS::remote::CommonResult result;
            if (shard_status != DSShardStatus::ReadWrite)
            {
                if (shard_status == DSShardStatus::Closed)
                {
                    result.set_error_code(::EloqDS::remote::DataStoreError::
                                              REQUESTED_NODE_NOT_OWNER);
                    result.set_error_msg("Requested data not on local node.");
                }
                else
                {
                    assert(shard_status == DSShardStatus::ReadOnly);
                    result.set_error_code(::EloqDS::remote::DataStoreError::
                                              WRITE_TO_READ_ONLY_DB);
                    result.set_error_msg("Write to read-only DB.");
                }
                batch_write_req->SetFinish(result);

                return;
            }

            std::shared_lock<std::shared_mutex> db_lk(db_mux_);
            auto db = GetDBPtr();
            if (!db)
            {
                DLOG(ERROR) << "DB is not opened";
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
                result.set_error_msg("DB is not opened");
                batch_write_req->SetFinish(result);
                return;
            }

            // Increase write counter before starting the write operation
            IncreaseWriteCounter();

            const uint16_t parts_cnt_per_key =
                batch_write_req->PartsCountPerKey();
            const uint16_t parts_cnt_per_record =
                batch_write_req->PartsCountPerRecord();
            const uint16_t key_parts_cnt =
                4 + parts_cnt_per_key;  // 4 for table name, partition id and
                                        // two separators
            const uint16_t record_parts_cnt =
                2 + parts_cnt_per_record;  // 2 for timestamp and ttl
            rocksdb::WriteOptions write_options;
            write_options.disableWAL = true;
            rocksdb::WriteBatch write_batch;

            std::unique_ptr<rocksdb::Slice[]> key_slices =
                std::make_unique<rocksdb::Slice[]>(key_parts_cnt);
            rocksdb::SliceParts key_parts(key_slices.get(), key_parts_cnt);
            std::string partition_id_str =
                std::to_string(batch_write_req->GetPartitionId());
            BuildKeyPrefixSlices(
                batch_write_req->GetTableName(), partition_id_str, key_slices);

            std::unique_ptr<rocksdb::Slice[]> value_slices =
                std::make_unique<rocksdb::Slice[]>(record_parts_cnt);
            for (size_t i = 0; i < batch_write_req->RecordsCount(); i++)
            {
                BuildKeySlices(
                    key_slices, batch_write_req, i, parts_cnt_per_key);

                // TODO(lzx):enable rocksdb user-defined-timestamp?
                if (batch_write_req->KeyOpType(i) == WriteOpType::DELETE)
                {
                    write_batch.Delete(key_parts);
                }
                else
                {
                    assert(batch_write_req->KeyOpType(i) == WriteOpType::PUT);
                    uint64_t rec_ts = batch_write_req->GetRecordTs(i);
                    uint64_t rec_ttl = batch_write_req->GetRecordTtl(i);
                    const uint16_t value_parts_size =
                        TransformRecordToValueSlices(batch_write_req,
                                                     i,
                                                     parts_cnt_per_record,
                                                     rec_ts,
                                                     rec_ttl,
                                                     value_slices);
                    rocksdb::SliceParts value_parts(value_slices.get(),
                                                    value_parts_size);
                    write_batch.Put(key_parts, value_parts);
                }

                // keep key prefix, and clear key part
                for (uint16_t i = 0; i < parts_cnt_per_key; i++)
                {
                    key_slices[4 + i].clear();
                }
                // clear value part
                for (uint16_t i = 0; i < record_parts_cnt; i++)
                {
                    value_slices[i].clear();
                }
            }

            auto write_status = db->Write(write_options, &write_batch);

            if (!write_status.ok())
            {
                LOG(ERROR) << "BatchWriteRecords failed, table:"
                           << batch_write_req->GetTableName()
                           << ", result:" << static_cast<int>(write_status.ok())
                           << ", error: " << write_status.ToString()
                           << ", error code: " << write_status.code();
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::WRITE_FAILED);
                result.set_error_msg(write_status.ToString());
            }
            else if (!batch_write_req->SkipWal())
            {
                rocksdb::FlushOptions flush_options;
                flush_options.wait = true;
                auto flush_status = db->Flush(flush_options);
                if (!flush_status.ok())
                {
                    LOG(ERROR)
                        << "Flush failed after BatchWriteRecords, error: "
                        << flush_status.ToString();
                    result.set_error_code(
                        ::EloqDS::remote::DataStoreError::FLUSH_FAILED);
                    result.set_error_msg(flush_status.ToString());
                }
            }

            DecreaseWriteCounter();
            result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
            batch_write_req->SetFinish(result);
        });
}

void RocksDBCloudDataStore::CreateTable(CreateTableRequest *create_table_req)
{
    std::unique_ptr<PoolableGuard> poolable_guard =
        std::make_unique<PoolableGuard>(create_table_req);

    ::EloqDS::remote::CommonResult result;
    result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
    create_table_req->SetFinish(result);
    return;
}

void RocksDBCloudDataStore::DropTable(DropTableRequest *drop_table_req)
{
    query_worker_pool_->SubmitWork(
        [this, drop_table_req]()
        {
            // Create a guard to ensure the poolable object is released to pool
            std::unique_ptr<PoolableGuard> poolable_guard =
                std::make_unique<PoolableGuard>(drop_table_req);

            // Increase write counter at the start of the operation
            IncreaseWriteCounter();

            ::EloqDS::remote::CommonResult result;
            std::shared_lock<std::shared_mutex> db_lk(db_mux_);
            auto db = GetDBPtr();
            if (!db)
            {
                // Decrease counter before error return
                DecreaseWriteCounter();
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
                result.set_error_msg("DB is not opened");
                drop_table_req->SetFinish(result);
                return;
            }

            const std::string_view table_name = drop_table_req->GetTableName();

            // build start key
            std::string start_key_str = BuildKey(table_name, 0, "");
            rocksdb::Slice start_key(start_key_str);

            // build end key
            std::string end_key_str = BuildKey(table_name, UINT32_MAX, "");
            end_key_str.back()++;
            rocksdb::Slice end_key(end_key_str);

            rocksdb::WriteOptions write_opts;
            write_opts.disableWAL = true;
            auto status = db->DeleteRange(write_opts, start_key, end_key);
            if (!status.ok())
            {
                LOG(ERROR) << "Unable to drop table with error: "
                           << status.ToString();

                // Decrease counter before error return
                DecreaseWriteCounter();
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::WRITE_FAILED);
                result.set_error_msg(status.ToString());
                drop_table_req->SetFinish(result);
                return;
            }

            rocksdb::FlushOptions flush_options;
            flush_options.allow_write_stall = true;
            flush_options.wait = true;

            status = db->Flush(flush_options);
            if (!status.ok())
            {
                LOG(ERROR) << "Unable to drop table with error: "
                           << status.ToString();
                // Decrease counter before error return
                DecreaseWriteCounter();
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::FLUSH_FAILED);
                result.set_error_msg(status.ToString());
                drop_table_req->SetFinish(result);
                return;
            }

            // Decrease counter before successful return
            DecreaseWriteCounter();
            result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
            drop_table_req->SetFinish(result);
            DLOG(INFO) << "DropTable successfully.";
        });
}

void RocksDBCloudDataStore::ScanNext(ScanRequest *scan_req)
{
    query_worker_pool_->SubmitWork(
        [this, scan_req]()
        {
            DLOG(INFO) << "RocksDBCloudDataStore::ScanNext "
                       << scan_req->GetPartitionId();
            PoolableGuard poolable_guard(scan_req);

            uint32_t partition_id = scan_req->GetPartitionId();
            bool scan_forward = scan_req->ScanForward();
            const std::string &session_id = scan_req->GetSessionId();
            scan_req->ClearSessionId();
            const bool inclusive_start = scan_req->InclusiveStart();
            const bool inclusive_end = scan_req->InclusiveEnd();
            const std::string_view start_key = scan_req->GetStartKey();
            std::string_view end_key = scan_req->GetEndKey();
            const size_t batch_size = scan_req->BatchSize();
            int search_cond_size = scan_req->GetSearchConditionsSize();

            std::shared_lock<std::shared_mutex> db_lk(db_mux_);

            TTLWrapper *iter_wrapper =
                data_store_service_->BorrowScanIter(shard_id_, session_id);

            auto shard_status = FetchDSShardStatus();
            if (shard_status != DSShardStatus::ReadOnly &&
                shard_status != DSShardStatus::ReadWrite)
            {
                if (iter_wrapper != nullptr)
                {
                    data_store_service_->EraseScanIter(shard_id_, session_id);
                }
                scan_req->SetFinish(
                    ::EloqDS::remote::DataStoreError::REQUESTED_NODE_NOT_OWNER);
                return;
            }

            std::string kv_start_key, kv_end_key;
            std::string key_prefix =
                BuildKeyPrefix(scan_req->GetTableName(), partition_id);

            // Prepare kv_start_key
            if (!start_key.empty())
            {
                // start key is not empty
                BuildKey(key_prefix, start_key, kv_start_key);
            }
            else if (scan_forward)
            {
                // scan forward and start key is empty
                // treat empty start key as the neg inf
                kv_start_key = key_prefix;
            }
            else
            {
                // scan backward and start key is empty
                // treat empty start key as the pos inf
                kv_start_key = key_prefix;
                kv_start_key.back()++;
            }

            // Prepare kv_end_key
            if (!end_key.empty())
            {
                // end key is not empty
                BuildKey(key_prefix, end_key, kv_end_key);
            }
            else if (scan_forward)
            {
                // scan forward and end key is empty
                // treat empty end key as the pos inf
                kv_end_key = key_prefix;
                kv_end_key.back()++;
            }
            else
            {
                // scan backward and end key is empty
                // treat empty end key as the neg inf
                kv_end_key = key_prefix;
            }

            rocksdb::Iterator *iter = nullptr;
            if (iter_wrapper != nullptr)
            {
                auto *rocksdb_iter_wrapper =
                    static_cast<RocksDBIteratorTTLWrapper *>(iter_wrapper);
                iter = rocksdb_iter_wrapper->GetIter();
            }
            else
            {
                rocksdb::ReadOptions read_options;
                // NOTICE: do not enable async_io if compiling rocksdbcloud
                // without iouring.
                read_options.async_io = false;
                iter = GetDBPtr()->NewIterator(read_options);

                rocksdb::Slice key(kv_start_key);
                if (scan_forward)
                {
                    iter->Seek(key);
                    if (!inclusive_start && iter->Valid())
                    {
                        rocksdb::Slice curr_key = iter->key();
                        if (curr_key.ToStringView() == key.ToStringView())
                        {
                            iter->Next();
                        }
                    }
                }
                else
                {
                    iter->SeekForPrev(key);
                    if (!inclusive_start && iter->Valid())
                    {
                        rocksdb::Slice curr_key = iter->key();
                        if (curr_key.ToStringView() == key.ToStringView())
                        {
                            iter->Prev();
                        }
                    }
                }
            }

            // Fetch the batch of records into the response
            uint32_t record_count = 0;
            bool scan_completed = false;
            while (iter->Valid() && record_count < batch_size)
            {
                if (scan_forward)
                {
                    if (!end_key.empty())
                    {
                        if (inclusive_end &&
                            iter->key().ToStringView() > kv_end_key)
                        {
                            scan_completed = true;
                            break;
                        }
                        else if (!inclusive_end &&
                                 iter->key().ToStringView() >= kv_end_key)
                        {
                            scan_completed = true;
                            break;
                        }
                    }
                    else
                    {
                        // end key is empty, then kv_end_key should not be
                        // available in the db
                        assert(iter->key().ToStringView() != kv_end_key);
                        if (iter->key().ToStringView() >= kv_end_key)
                        {
                            scan_completed = true;
                            break;
                        }
                    }
                }
                else
                {
                    if (!end_key.empty())
                    {
                        if (inclusive_end &&
                            iter->key().ToStringView() < kv_end_key)
                        {
                            scan_completed = true;
                            break;
                        }
                        else if (!inclusive_end &&
                                 iter->key().ToStringView() <= kv_end_key)
                        {
                            scan_completed = true;
                            break;
                        }
                    }
                    else
                    {
                        // end key is empty, then kv_end_key should not be
                        // available in the db
                        assert(iter->key().ToStringView() != kv_end_key);
                        if (iter->key().ToStringView() <= kv_end_key)
                        {
                            scan_completed = true;
                            break;
                        }
                    }
                }

                // NOTICE: must remove prefix from store-key
                rocksdb::Slice key_slice = iter->key();
                assert(key_slice.size() > 0);
                // Deserialize key_slice to key by removing prefix
                std::string_view key;
                [[maybe_unused]] bool ret =
                    DeserializeKey(key_slice.data(),
                                   key_slice.size(),
                                   scan_req->GetTableName(),
                                   scan_req->GetPartitionId(),
                                   key);
                assert(ret);
                rocksdb::Slice value = iter->value();
                const remote::SearchCondition *cond = nullptr;
                bool matched = true;
                for (int cond_idx = 0; cond_idx < search_cond_size; ++cond_idx)
                {
                    cond = scan_req->GetSearchConditions(cond_idx);
                    assert(cond);
                    if (cond->field_name() == "type" &&
                        cond->value().compare(0, 1, value.data(), 0, 1))
                    {
                        // type mismatch
                        matched = false;
                        break;
                    }
                }
                if (!matched)
                {
                    if (scan_forward)
                    {
                        iter->Next();
                    }
                    else
                    {
                        iter->Prev();
                    }
                    continue;
                }

                // Deserialize value to record and record_ts
                std::string rec;
                uint64_t rec_ts;
                uint64_t rec_ttl;
                DeserializeValueToRecord(
                    value.data(), value.size(), rec, rec_ts, rec_ttl);

                scan_req->AddItem(
                    std::string(key), std::move(rec), rec_ts, rec_ttl);
                if (scan_forward)
                {
                    iter->Next();
                }
                else
                {
                    iter->Prev();
                }
                record_count++;
            }

            if (!iter->Valid() || scan_completed)
            {
                // run out of records, remove iter
                if (iter_wrapper != nullptr)
                {
                    data_store_service_->EraseScanIter(shard_id_, session_id);
                }
                else
                {
                    delete iter;
                }
            }
            else
            {
                if (iter_wrapper != nullptr)
                {
                    data_store_service_->ReturnScanIter(shard_id_,
                                                        iter_wrapper);
                    // Set session id carry over to the response
                    scan_req->SetSessionId(session_id);
                }
                else
                {
                    // Otherwise, save the iterator in the session map
                    auto iter_wrapper =
                        std::make_unique<RocksDBIteratorTTLWrapper>(iter);
                    std::string session_id =
                        data_store_service_->GenerateSessionId();
                    // Set session id in the response
                    scan_req->SetSessionId(session_id);
                    // Save the iterator in the session map
                    data_store_service_->EmplaceScanIter(
                        shard_id_, session_id, std::move(iter_wrapper));
                }
            }

            scan_req->SetFinish(::EloqDS::remote::DataStoreError::NO_ERROR);
        });
}

void RocksDBCloudDataStore::ScanClose(ScanRequest *scan_req)
{
    query_worker_pool_->SubmitWork(
        [this, scan_req]()
        {
            // Create a guard to ensure the poolable object is released to pool
            PoolableGuard self_guard(scan_req);

            const std::string &session_id = scan_req->GetSessionId();
            if (!session_id.empty())
            {
                // Erase the iterator from the session map
                data_store_service_->EraseScanIter(shard_id_, session_id);
            }

            scan_req->SetFinish(::EloqDS::remote::DataStoreError::NO_ERROR);
        });
}

void RocksDBCloudDataStore::WaitForPendingWrites()
{
    // Wait until all ongoing write requests are completed
    while (ongoing_write_requests_.load(std::memory_order_acquire) > 0)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

DSShardStatus RocksDBCloudDataStore::FetchDSShardStatus() const
{
    assert(data_store_service_ != nullptr);
    return data_store_service_->FetchDSShardStatus(shard_id_);
}

void RocksDBCloudDataStore::IncreaseWriteCounter()
{
    ongoing_write_requests_.fetch_add(1, std::memory_order_release);
}

void RocksDBCloudDataStore::DecreaseWriteCounter()
{
    ongoing_write_requests_.fetch_sub(1, std::memory_order_release);
}

void RocksDBCloudDataStore::SwitchToReadOnly()
{
    WaitForPendingWrites();

    bthread::Mutex mutex;
    bthread::ConditionVariable cond_var;
    bool done = false;

    // pause all background jobs to stop compaction and obselete file
    // deletion
    query_worker_pool_->SubmitWork(
        [this, &mutex, &cond_var, &done]()
        {
            // Run pause background work in a separate thread to avoid blocking
            // bthread worker threads
            std::shared_lock<std::shared_mutex> db_lk(db_mux_);
            auto db = GetDBPtr();
            if (db != nullptr)
            {
                db->PauseBackgroundWork();
            }

            std::unique_lock<bthread::Mutex> lk(mutex);
            done = true;
            cond_var.notify_one();
        });

    std::unique_lock<bthread::Mutex> lk(mutex);

    while (!done)
    {
        cond_var.wait(lk);
    }
}

void RocksDBCloudDataStore::SwitchToReadWrite()
{
    // Since ContinueBackgroundWork() is non-blocking, we don't need the
    // thread synchronization machinery here
    std::shared_lock<std::shared_mutex> db_lk(db_mux_);
    auto db = GetDBPtr();
    if (db != nullptr)
    {
        db->ContinueBackgroundWork();
    }
}

}  // namespace EloqDS
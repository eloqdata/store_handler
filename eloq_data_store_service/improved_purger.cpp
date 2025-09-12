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

#include <chrono>
#include <iostream>
#include <limits>
#include <memory>
#include <regex>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "cloud/cloud_manifest.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "file/filename.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"

#include "purger_sliding_window.h"

// Command line flags
DEFINE_string(s3_url, "", "S3 URL in format s3://bucket/path (required)");
DEFINE_int32(purge_interval_seconds, 300, "Purge cycle interval in seconds (default: 5 minutes)");
DEFINE_bool(dry_run, false, "Dry run mode - list obsolete files but don't delete them");
DEFINE_string(aws_region, "us-west-2", "AWS region (default: us-west-2)");
DEFINE_int32(file_number_grace_minutes, 5, "Grace period for file number threshold in minutes (default: 5)");

namespace ROCKSDB_NAMESPACE {

/**
 * @brief Parse S3 URL into bucket and object path components
 * @param s3_url S3 URL in format s3://bucket/path
 * @param bucket_name Output bucket name
 * @param object_path Output object path
 * @return true if parsing succeeded, false otherwise
 */
bool ParseS3Url(const std::string& s3_url, std::string* bucket_name, std::string* object_path) {
    std::regex s3_regex(R"(s3://([^/]+)(/.*)?)", std::regex_constants::icase);
    std::smatch matches;

    if (!std::regex_match(s3_url, matches, s3_regex)) {
        return false;
    }

    *bucket_name = matches[1].str();
    *object_path = matches.size() > 2 ? matches[2].str() : "";

    // Remove leading slash from object path
    if (!object_path->empty() && (*object_path)[0] == '/') {
        *object_path = object_path->substr(1);
    }

    return true;
}

/**
 * @brief Enhanced purger with file number threshold support
 */
class ImprovedPurger {
public:
    // Type aliases
    using PurgerAllFiles = std::vector<std::pair<std::string, CloudObjectInformation>>;
    using PurgerCloudManifestMap = std::unordered_map<std::string, std::unique_ptr<CloudManifest>>;
    using PurgerLiveFileSet = std::unordered_set<std::string>;
    using PurgerEpochManifestMap = std::unordered_map<std::string, CloudObjectInformation>;
    using PurgerFileNumberThresholds = std::unordered_map<std::string, uint64_t>; // epoch -> threshold

    struct PurgerCycleState {
        PurgerAllFiles all_files;
        std::vector<std::string> cloud_manifest_files;
        PurgerCloudManifestMap cloudmanifests;
        PurgerLiveFileSet live_file_names;
        PurgerEpochManifestMap current_epoch_manifest_files;
        PurgerFileNumberThresholds file_number_thresholds; // NEW: epoch -> min file number
        std::vector<std::string> obsolete_files;
    };

private:
    std::shared_ptr<CloudFileSystemImpl> cfs_;
    std::string bucket_name_;
    std::string object_path_;
    bool dry_run_;
    int file_number_grace_minutes_;

public:
    ImprovedPurger(std::shared_ptr<CloudFileSystemImpl> cfs,
                   const std::string& bucket_name,
                   const std::string& object_path,
                   bool dry_run,
                   int file_number_grace_minutes)
        : cfs_(cfs),
          bucket_name_(bucket_name),
          object_path_(object_path),
          dry_run_(dry_run),
          file_number_grace_minutes_(file_number_grace_minutes) {}

    /**
     * @brief Run a single purge cycle with improved file number checking
     */
    void RunSinglePurgeCycle() {
        PurgerCycleState state;

        LOG(INFO) << "[ImprovedPurger] Starting purge cycle for " << bucket_name_ << "/" << object_path_;

        if (!ListAllFiles(&state.all_files)) {
            return;
        }

        if (!ListCloudManifests(&state.cloud_manifest_files)) {
            return;
        }

        if (!LoadCloudManifests(state.cloud_manifest_files, &state.cloudmanifests)) {
            return;
        }

        if (!CollectLiveFiles(state.cloudmanifests, &state.live_file_names,
                             &state.current_epoch_manifest_files)) {
            return;
        }

        // NEW: Load file number thresholds from S3
        LoadFileNumberThresholds(state.cloudmanifests, &state.file_number_thresholds);

        // Enhanced selection with file number checking
        SelectObsoleteFilesWithThreshold(state.all_files, state.live_file_names,
                                        state.current_epoch_manifest_files,
                                        state.file_number_thresholds,
                                        &state.obsolete_files);

        if (dry_run_) {
            LOG(INFO) << "[ImprovedPurger] DRY RUN: Would delete " << state.obsolete_files.size() << " files";
            for (const auto& file : state.obsolete_files) {
                LOG(INFO) << "[ImprovedPurger] DRY RUN: Would delete " << file;
            }
        } else {
            DeleteObsoleteFiles(state.obsolete_files);
        }

        LOG(INFO) << "[ImprovedPurger] Purge cycle summary: total_files=" << state.all_files.size()
                  << " manifests=" << state.cloudmanifests.size()
                  << " live_files=" << state.live_file_names.size()
                  << " obsolete_selected=" << state.obsolete_files.size()
                  << " thresholds_loaded=" << state.file_number_thresholds.size();
    }

private:
    bool ListAllFiles(PurgerAllFiles* all_files) {
        IOStatus s = cfs_->GetStorageProvider()->ListCloudObjects(
            bucket_name_, object_path_, all_files);

        if (!s.ok()) {
            LOG(ERROR) << "[ImprovedPurger] Failed to list files in " << object_path_
                       << ": " << s.ToString();
            return false;
        }

        LOG(INFO) << "[ImprovedPurger] Listed " << all_files->size() << " files";
        return true;
    }

    bool ListCloudManifests(std::vector<std::string>* cloud_manifest_files) {
        IOStatus s = cfs_->GetStorageProvider()->ListCloudObjectsWithPrefix(
            bucket_name_, object_path_, "CLOUDMANIFEST", cloud_manifest_files);

        if (!s.ok()) {
            LOG(ERROR) << "[ImprovedPurger] Failed to list cloud manifests: " << s.ToString();
            return false;
        }

        LOG(INFO) << "[ImprovedPurger] Found " << cloud_manifest_files->size() << " cloud manifest files";
        return true;
    }

    bool LoadCloudManifests(const std::vector<std::string>& cloud_manifest_files,
                           PurgerCloudManifestMap* manifests) {
        const FileOptions file_opts;
        IODebugContext* dbg = nullptr;
        bool success = true;

        for (const auto& cloud_manifest_file : cloud_manifest_files) {
            std::string full_path = object_path_ + "/" + cloud_manifest_file;
            std::unique_ptr<FSSequentialFile> file;

            IOStatus s = cfs_->NewSequentialFileCloud(bucket_name_, full_path, file_opts, &file, dbg);
            if (!s.ok()) {
                LOG(ERROR) << "[ImprovedPurger] Failed to open manifest " << cloud_manifest_file
                           << ": " << s.ToString();
                success = false;
                continue;
            }

            std::unique_ptr<CloudManifest> cloud_manifest;
            s = CloudManifest::LoadFromLog(
                std::unique_ptr<SequentialFileReader>(
                    new SequentialFileReader(std::move(file), cloud_manifest_file)),
                &cloud_manifest);

            if (!s.ok()) {
                LOG(ERROR) << "[ImprovedPurger] Failed to load manifest " << cloud_manifest_file
                           << ": " << s.ToString();
                success = false;
                continue;
            }

            LOG(INFO) << "[ImprovedPurger] Loaded manifest " << cloud_manifest_file
                      << " with epoch " << cloud_manifest->GetCurrentEpoch();

            (*manifests)[cloud_manifest_file] = std::move(cloud_manifest);
        }

        return success;
    }

    bool CollectLiveFiles(const PurgerCloudManifestMap& cloudmanifests,
                         PurgerLiveFileSet* live_files,
                         PurgerEpochManifestMap* epoch_manifest_infos) {
        std::unique_ptr<ManifestReader> manifest_reader(
            new ManifestReader(cfs_->info_log_, cfs_.get(), bucket_name_));

        std::set<uint64_t> live_file_numbers;
        bool success = true;

        for (const auto& entry : cloudmanifests) {
            const std::string& cloud_manifest_name = entry.first;
            CloudManifest* cloud_manifest_ptr = entry.second.get();

            live_file_numbers.clear();
            std::string current_epoch = cloud_manifest_ptr->GetCurrentEpoch();
            std::string manifest_file = ManifestFileWithEpoch(object_path_, current_epoch);

            CloudObjectInformation manifest_file_info;
            IOStatus s = cfs_->GetStorageProvider()->GetCloudObjectMetadata(
                bucket_name_, manifest_file, &manifest_file_info);

            if (!s.ok()) {
                LOG(ERROR) << "[ImprovedPurger] Failed to get metadata for manifest "
                           << manifest_file << ": " << s.ToString();
                success = false;
                continue;
            }

            (*epoch_manifest_infos)[current_epoch] = manifest_file_info;

            s = manifest_reader->GetLiveFiles(object_path_, current_epoch, &live_file_numbers);
            if (!s.ok()) {
                LOG(ERROR) << "[ImprovedPurger] Failed to get live files from manifest "
                           << cloud_manifest_name << ": " << s.ToString();
                success = false;
                continue;
            }

            for (uint64_t num : live_file_numbers) {
                std::string file_name = MakeTableFileName(num);
                file_name = cfs_->RemapFilenameWithCloudManifest(file_name, cloud_manifest_ptr);
                live_files->insert(file_name);
                DLOG(INFO) << "[ImprovedPurger] Live file: " << file_name;
            }
        }

        return success;
    }

    void LoadFileNumberThresholds(const PurgerCloudManifestMap& cloudmanifests,
                                 PurgerFileNumberThresholds* thresholds) {
        for (const auto& entry : cloudmanifests) {
            CloudManifest* manifest = entry.second.get();
            std::string epoch = manifest->GetCurrentEpoch();

            // Create S3 file number updater to read threshold
            auto s3_updater = std::make_unique<EloqDS::S3FileNumberUpdater>(
                bucket_name_, object_path_, epoch, cfs_->GetStorageProvider());

            uint64_t threshold = s3_updater->ReadSmallestFileNumber();
            (*thresholds)[epoch] = threshold;

            if (threshold == std::numeric_limits<uint64_t>::max()) {
                LOG(INFO) << "[ImprovedPurger] No file number threshold found for epoch " << epoch
                          << " (using conservative approach)";
            } else {
                LOG(INFO) << "[ImprovedPurger] Loaded file number threshold " << threshold
                          << " for epoch " << epoch;
            }
        }
    }

    void SelectObsoleteFilesWithThreshold(const PurgerAllFiles& all_files,
                                         const PurgerLiveFileSet& live_files,
                                         const PurgerEpochManifestMap& epoch_manifest_infos,
                                         const PurgerFileNumberThresholds& thresholds,
                                         std::vector<std::string>* obsolete_files) {
        for (const auto& candidate : all_files) {
            const std::string& candidate_file_path = candidate.first;
            const CloudObjectInformation& candidate_file_info = candidate.second;

            // Skip non-SST files
            if (!ends_with(RemoveEpoch(candidate_file_path), ".sst")) {
                continue;
            }

            // Skip live files
            if (live_files.find(candidate_file_path) != live_files.end()) {
                continue;
            }

            std::string candidate_epoch = GetEpoch(candidate_file_path);
            uint64_t candidate_modification_time = candidate_file_info.modification_time;

            // Get manifest modification time
            uint64_t manifest_modification_time = std::numeric_limits<uint64_t>::max();
            auto epoch_it = epoch_manifest_infos.find(candidate_epoch);
            if (epoch_it != epoch_manifest_infos.end()) {
                manifest_modification_time = epoch_it->second.modification_time;
            }

            // NEW: Check file number threshold
            bool safe_by_file_number = true;
            auto threshold_it = thresholds.find(candidate_epoch);
            if (threshold_it != thresholds.end()) {
                uint64_t threshold = threshold_it->second;
                if (threshold != std::numeric_limits<uint64_t>::max()) {
                    // Extract file number from candidate file name
                    uint64_t file_number = 0;
                    std::string base_name = RemoveEpoch(candidate_file_path);
                    if (ParseFileName(base_name, &file_number, nullptr)) {
                        if (file_number >= threshold) {
                            safe_by_file_number = false;
                            DLOG(INFO) << "[ImprovedPurger] File " << candidate_file_path
                                       << " protected by file number threshold (file_num="
                                       << file_number << ", threshold=" << threshold << ")";
                        }
                    }
                }
            }

            // Apply both time-based and file number-based checks
            if (safe_by_file_number && candidate_modification_time < manifest_modification_time) {
                obsolete_files->push_back(candidate_file_path);
                DLOG(INFO) << "[ImprovedPurger] File " << candidate_file_path << " is obsolete";
            } else {
                DLOG(INFO) << "[ImprovedPurger] File " << candidate_file_path
                           << " is protected (safe_by_file_number=" << safe_by_file_number
                           << ", candidate_time=" << candidate_modification_time
                           << ", manifest_time=" << manifest_modification_time << ")";
            }
        }
    }

    void DeleteObsoleteFiles(const std::vector<std::string>& obsolete_files) {
        size_t deleted = 0;
        size_t failures = 0;

        for (const auto& file_to_delete : obsolete_files) {
            std::string file_path = object_path_ + "/" + file_to_delete;
            LOG(INFO) << "[ImprovedPurger] Deleting obsolete file " << file_to_delete;

            IOStatus s = cfs_->GetStorageProvider()->DeleteCloudObject(bucket_name_, file_path);
            if (!s.ok()) {
                ++failures;
                LOG(ERROR) << "[ImprovedPurger] Failed to delete " << file_path
                           << ": " << s.ToString();
            } else {
                ++deleted;
            }
        }

        LOG(INFO) << "[ImprovedPurger] Deletion summary: requested=" << obsolete_files.size()
                  << " deleted=" << deleted << " failures=" << failures;
    }
};

} // namespace ROCKSDB_NAMESPACE

int main(int argc, char* argv[]) {
    // Initialize gflags and glog
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    // Set log level to INFO by default
    FLAGS_logtostderr = 1;
    FLAGS_minloglevel = 0; // INFO level

    if (FLAGS_s3_url.empty()) {
        std::cerr << "Error: --s3_url is required\n";
        std::cerr << "Usage: " << argv[0] << " --s3_url=s3://bucket/path [options]\n";
        std::cerr << "Options:\n";
        std::cerr << "  --purge_interval_seconds=300     Purge cycle interval (default: 5 minutes)\n";
        std::cerr << "  --dry_run=false                  Dry run mode - don't actually delete files\n";
        std::cerr << "  --aws_region=us-west-2          AWS region\n";
        std::cerr << "  --file_number_grace_minutes=5    Grace period for file number threshold\n";
        return 1;
    }

    std::string bucket_name, object_path;
    if (!ROCKSDB_NAMESPACE::ParseS3Url(FLAGS_s3_url, &bucket_name, &object_path)) {
        std::cerr << "Error: Invalid S3 URL format. Expected: s3://bucket/path\n";
        return 1;
    }

    LOG(INFO) << "Starting improved purger for S3 URL: " << FLAGS_s3_url;
    LOG(INFO) << "Parsed - Bucket: " << bucket_name << ", Object Path: " << object_path;
    LOG(INFO) << "Configuration - Purge Interval: " << FLAGS_purge_interval_seconds
              << "s, Dry Run: " << (FLAGS_dry_run ? "true" : "false")
              << ", AWS Region: " << FLAGS_aws_region
              << ", File Number Grace: " << FLAGS_file_number_grace_minutes << " minutes";

    try {
        // Create CloudFileSystemOptions
        ROCKSDB_NAMESPACE::CloudFileSystemOptions cfs_options;
        cfs_options.src_bucket.SetBucketName(bucket_name, "");
        cfs_options.src_bucket.SetRegion(FLAGS_aws_region);
        cfs_options.dest_bucket = cfs_options.src_bucket;
        cfs_options.dest_bucket.SetObjectPath(object_path);
        cfs_options.purger_periodicity_millis = FLAGS_purge_interval_seconds * 1000;

        // Create CloudFileSystem
        std::shared_ptr<ROCKSDB_NAMESPACE::CloudFileSystem> cloud_fs;
        ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::CloudFileSystem::NewAwsFileSystem(
            ROCKSDB_NAMESPACE::FileSystem::Default(), cfs_options, &cloud_fs);

        if (!s.ok()) {
            LOG(FATAL) << "Failed to create CloudFileSystem: " << s.ToString();
            return 1;
        }

        auto cfs_impl = std::dynamic_pointer_cast<ROCKSDB_NAMESPACE::CloudFileSystemImpl>(cloud_fs);
        if (!cfs_impl) {
            LOG(FATAL) << "Failed to cast to CloudFileSystemImpl";
            return 1;
        }

        // Create and run improved purger
        ROCKSDB_NAMESPACE::ImprovedPurger purger(
            cfs_impl, bucket_name, object_path,
            FLAGS_dry_run, FLAGS_file_number_grace_minutes);

        // Run purge cycles
        while (true) {
            auto start_time = std::chrono::steady_clock::now();

            purger.RunSinglePurgeCycle();

            auto end_time = std::chrono::steady_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            LOG(INFO) << "Purge cycle completed in " << duration.count() << "ms";

            // Sleep until next cycle
            std::this_thread::sleep_for(std::chrono::seconds(FLAGS_purge_interval_seconds));
        }

    } catch (const std::exception& e) {
        LOG(FATAL) << "Exception: " << e.what();
        return 1;
    }

    return 0;
}


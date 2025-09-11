// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include <chrono>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cloud/cloud_manifest.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "cloud/purge.h"
#include "file/filename.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider.h"

/**
 * Purger implementation for the single object path case. This refactored
 * version separates the main loop (Purger) from a single purge cycle
 * (RunSinglePurgeCycle) and uses helper methods for each logical step.
 *
 * Prerequisites for running the purger remain unchanged: it only runs when
 * the source and destination buckets are either the same or not both valid
 * with non-empty object paths that differ. If both are valid and differ,
 * we skip running.
 */
namespace ROCKSDB_NAMESPACE {

namespace {  // anonymous namespace for refactored free helper utilities

// Type aliases local to this translation unit (were previously in header)
using PurgerAllFiles =
    std::vector<std::pair<std::string, CloudObjectInformation>>;
using PurgerCloudManifestMap =
    std::unordered_map<std::string, std::unique_ptr<CloudManifest>>;
using PurgerLiveFileSet = std::unordered_set<std::string>;
using PurgerEpochManifestMap =
    std::unordered_map<std::string, CloudObjectInformation>;

struct PurgerCycleState {
  PurgerAllFiles all_files;  // (name, metadata)
  std::vector<std::string>
      cloud_manifest_files;               // names of CLOUDMANIFEST* objects
  PurgerCloudManifestMap cloudmanifests;  // loaded cloud manifest objects
  PurgerLiveFileSet live_file_names;      // logical filenames considered live
  PurgerEpochManifestMap
      current_epoch_manifest_files;         // epoch -> manifest file metadata
  std::vector<std::string> obsolete_files;  // files selected for deletion
};

static bool PrerequisitesMet(const CloudFileSystemImpl &cfs) {
  const CloudFileSystemOptions &cfs_opts = cfs.GetCloudFileSystemOptions();
  if (cfs_opts.src_bucket.IsValid() &&
      !cfs_opts.src_bucket.GetObjectPath().empty() &&
      cfs_opts.dest_bucket.IsValid() &&
      !cfs_opts.dest_bucket.GetObjectPath().empty() &&
      cfs_opts.src_bucket != cfs_opts.dest_bucket) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs.info_log_,
        "[pg] Single Object Path Purger is not running because the "
        "prerequisites are not met.");
    return false;
  }
  return true;
}

static IOStatus ListAllFiles(CloudFileSystemImpl &cfs,
                             PurgerAllFiles *all_files) {
  const std::string &dest_object_path = cfs.GetDestObjectPath();
  IOStatus s = cfs.GetStorageProvider()->ListCloudObjects(
      cfs.GetDestBucketName(), dest_object_path, all_files);
  if (!s.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs.info_log_,
        "[pg] Failed to list files in destination object path %s: %s",
        dest_object_path.c_str(), s.ToString().c_str());
  }

  return s;
}

static IOStatus ListCloudManifests(
    CloudFileSystemImpl &cfs, std::vector<std::string> *cloud_manifest_files) {
  IOStatus s = cfs.GetStorageProvider()->ListCloudObjectsWithPrefix(
      cfs.GetDestBucketName(), cfs.GetDestObjectPath(), "CLOUDMANIFEST",
      cloud_manifest_files);
  if (!s.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs.info_log_,
        "[pg] Failed to list cloud manifest files in bucket %s: %s",
        cfs.GetDestBucketName().c_str(), s.ToString().c_str());
  } else {
    for (const auto &f : *cloud_manifest_files) {
      Log(InfoLogLevel::INFO_LEVEL, cfs.info_log_,
          "[pg] Found cloud manifest file %s", f.c_str());
    }
  }
  return s;
}

static IOStatus LoadCloudManifests(
    CloudFileSystemImpl &cfs,
    const std::vector<std::string> &cloud_manifest_files,
    PurgerCloudManifestMap *manifests) {
  const FileOptions file_opts;
  IODebugContext *dbg = nullptr;
  IOStatus overall = IOStatus::OK();

  for (const auto &cloud_manifest_file : cloud_manifest_files) {
    std::string cloud_manifest_file_path =
        cfs.GetDestObjectPath() + pathsep + cloud_manifest_file;
    std::unique_ptr<FSSequentialFile> file;
    IOStatus s = cfs.NewSequentialFileCloud(cfs.GetDestBucketName(),
                                            cloud_manifest_file_path, file_opts,
                                            &file, dbg);
    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, cfs.info_log_,
          "[pg] Failed to open cloud manifest file %s: %s",
          cloud_manifest_file.c_str(), s.ToString().c_str());
      if (overall.ok()) overall = s;
      continue;
    }

    std::unique_ptr<CloudManifest> cloud_manifest;
    s = CloudManifest::LoadFromLog(
        std::unique_ptr<SequentialFileReader>(
            new SequentialFileReader(std::move(file), cloud_manifest_file)),
        &cloud_manifest);
    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, cfs.info_log_,
          "[pg] Failed to load cloud manifest from file %s: %s",
          cloud_manifest_file.c_str(), s.ToString().c_str());
      if (overall.ok()) overall = s;
      continue;
    }

    Log(InfoLogLevel::INFO_LEVEL, cfs.info_log_,
        "[pg] Loaded cloud manifest file %s with current epoch %s",
        cloud_manifest_file.c_str(), cloud_manifest->GetCurrentEpoch().c_str());

    (*manifests)[cloud_manifest_file] = std::move(cloud_manifest);
  }
  return overall;
}

static IOStatus CollectLiveFiles(CloudFileSystemImpl &cfs,
                                 const PurgerCloudManifestMap &cloudmanifests,
                                 PurgerLiveFileSet *live_files,
                                 PurgerEpochManifestMap *epoch_manifest_infos) {
  const CloudFileSystemOptions &cfs_opts = cfs.GetCloudFileSystemOptions();
  const std::string &dest_object_path = cfs_opts.dest_bucket.GetObjectPath();
  IOStatus overall = IOStatus::OK();

  std::unique_ptr<ManifestReader> manifest_reader(new ManifestReader(
      cfs.info_log_, &cfs, cfs_opts.dest_bucket.GetBucketName()));

  std::set<uint64_t>
      live_file_numbers;  // temporary container reused per manifest

  for (auto &entry : cloudmanifests) {
    const auto &cloud_manifest_name = entry.first;
    CloudManifest *cloud_manifest_ptr = entry.second.get();

    live_file_numbers.clear();

    std::string current_epoch = cloud_manifest_ptr->GetCurrentEpoch();
    auto manifest_file = ManifestFileWithEpoch(dest_object_path, current_epoch);

    CloudObjectInformation manifest_file_info;
    IOStatus s = cfs.GetStorageProvider()->GetCloudObjectMetadata(
        cfs.GetDestBucketName(), manifest_file, &manifest_file_info);

    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, cfs.info_log_,
          "[pg] Failed to get metadata for manifest file %s: %s",
          manifest_file.c_str(), s.ToString().c_str());
      if (overall.ok()) overall = s;
      continue;
    }

    Log(InfoLogLevel::DEBUG_LEVEL, cfs.info_log_,
        "[pg] Current epoch Manifest file %s of CloudManifest %s has size %lu "
        "and content hash %s and timestamp %lu",
        manifest_file.c_str(), cloud_manifest_name.c_str(),
        manifest_file_info.size, manifest_file_info.content_hash.c_str(),
        manifest_file_info.modification_time);

    (*epoch_manifest_infos)[current_epoch] = manifest_file_info;

    s = manifest_reader->GetLiveFiles(dest_object_path, current_epoch,
                                      &live_file_numbers);
    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, cfs.info_log_,
          "[pg] Failed to get live files from cloud manifest file %s: %s",
          cloud_manifest_name.c_str(), s.ToString().c_str());
      if (overall.ok()) overall = s;
      continue;
    }

    for (const auto &num : live_file_numbers) {
      std::string file_name = MakeTableFileName(num);
      file_name =
          cfs.RemapFilenameWithCloudManifest(file_name, cloud_manifest_ptr);
      live_files->insert(file_name);
      Log(InfoLogLevel::DEBUG_LEVEL, cfs.info_log_,
          "[pg] Live file %s found in cloud manifest %s", file_name.c_str(),
          cloud_manifest_name.c_str());
    }
  }
  return overall;
}

static void SelectObsoleteFiles(
    CloudFileSystemImpl &cfs, const PurgerAllFiles &all_files,
    const PurgerLiveFileSet &live_files,
    const PurgerEpochManifestMap &epoch_manifest_infos,
    std::vector<std::string> *obsolete_files) {
  for (const auto &candidate : all_files) {
    Log(InfoLogLevel::DEBUG_LEVEL, cfs.info_log_,
        "[pg] Checking candidate file %s", candidate.first.c_str());
    const std::string &candidate_file_path = candidate.first;

    // Skip files that are not SST files
    if (!ends_with(RemoveEpoch(candidate_file_path), ".sst")) {
      continue;
    }

    const std::string candidate_file_epoch = GetEpoch(candidate_file_path);
    const CloudObjectInformation &candidate_file_info = candidate.second;
    uint64_t candidate_modification_time =
        candidate_file_info.modification_time;

    // Give max value to manifest modification time
    // if the candidate file epoch is not current epoch
    uint64_t manifest_modification_time = std::numeric_limits<uint64_t>::max();
    auto it_epoch = epoch_manifest_infos.find(candidate_file_epoch);
    if (it_epoch != epoch_manifest_infos.end()) {
      manifest_modification_time = it_epoch->second.modification_time;
    }

    if (live_files.find(candidate_file_path) != live_files.end()) {
      continue;
    }

    if (candidate_modification_time < manifest_modification_time) {
      obsolete_files->push_back(candidate_file_path);
      Log(InfoLogLevel::DEBUG_LEVEL, cfs.info_log_,
          "[pg] Candidate file %s is obsolete and will be deleted",
          candidate_file_path.c_str());
    } else {
      Log(InfoLogLevel::DEBUG_LEVEL, cfs.info_log_,
          "[pg] Candidate file %s is not obsolete because its modification "
          "time %lu is later than the current epoch manifest file's "
          "modification time %lu",
          candidate_file_path.c_str(), candidate_modification_time,
          manifest_modification_time);
    }
  }
}

static void DeleteObsoleteFiles(
    CloudFileSystemImpl &cfs, const std::vector<std::string> &obsolete_files) {
  const std::string &dest_object_path = cfs.GetDestObjectPath();
  size_t deleted = 0;
  size_t failures = 0;
  for (const auto &file_to_delete : obsolete_files) {
    std::string file_path = dest_object_path + pathsep + file_to_delete;
    Log(InfoLogLevel::INFO_LEVEL, cfs.info_log_,
        "[pg] Deleting obsolete file %s from destination bucket",
        file_to_delete.c_str());
    IOStatus s = cfs.GetStorageProvider()->DeleteCloudObject(
        cfs.GetDestBucketName(), file_path);
    if (!s.ok()) {
      ++failures;
      Log(InfoLogLevel::ERROR_LEVEL, cfs.info_log_,
          "[pg] Failed to delete obsolete file %s: %s", file_path.c_str(),
          s.ToString().c_str());
    } else {
      ++deleted;
    }
  }
  Log(InfoLogLevel::DEBUG_LEVEL, cfs.info_log_,
      "[pg] Obsolete deletion summary: requested=%zu deleted=%zu failures=%zu",
      obsolete_files.size(), deleted, failures);
}

static void RunSinglePurgeCycle(CloudFileSystemImpl &cfs) {
  PurgerCycleState state;  // fresh state each cycle

  Log(InfoLogLevel::INFO_LEVEL, cfs.info_log_,
      "[pg] Single Object Path Purger started a new cycle");

  if (!ListAllFiles(cfs, &state.all_files).ok()) {
    return;
  }

  if (!ListCloudManifests(cfs, &state.cloud_manifest_files).ok()) {
    return;
  }

  if (!LoadCloudManifests(cfs, state.cloud_manifest_files,
                          &state.cloudmanifests)
           .ok()) {
    return;
  }

  if (!CollectLiveFiles(cfs, state.cloudmanifests, &state.live_file_names,
                        &state.current_epoch_manifest_files)
           .ok()) {
    return;
  }

  SelectObsoleteFiles(cfs, state.all_files, state.live_file_names,
                      state.current_epoch_manifest_files,
                      &state.obsolete_files);

  DeleteObsoleteFiles(cfs, state.obsolete_files);

  Log(InfoLogLevel::INFO_LEVEL, cfs.info_log_,
      "[pg] Purge cycle summary: total_listed=%zu manifests_listed=%zu "
      "manifests_loaded=%zu live_files=%zu obsolete_selected=%zu",
      state.all_files.size(), state.cloud_manifest_files.size(),
      state.cloudmanifests.size(), state.live_file_names.size(),
      state.obsolete_files.size());
}

}  // anonymous namespace

// ------------- Main purger thread ------------- //

void CloudFileSystemImpl::Purger() {
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[pg] Single Object Path Purger thread started");

  if (!PrerequisitesMet(*this)) {
    return;
  }

  const auto periodicity_ms =
      GetCloudFileSystemOptions().purger_periodicity_millis;

  while (true) {
    // Wait for next cycle or termination request
    std::unique_lock<std::mutex> lk(purger_lock_);
    purger_cv_.wait_for(lk, std::chrono::milliseconds(periodicity_ms),
                        [&]() { return !purger_is_running_; });
    if (!purger_is_running_) {
      break;  // shutdown requested
    }
    lk.unlock();  // release lock during IO work

    RunSinglePurgeCycle(*this);
  }

  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[pg] Single Object Path Purger thread exiting");
}

IOStatus CloudFileSystemImpl::FindObsoleteFiles(
    const std::string & /*bucket_name_prefix*/,
    std::vector<std::string> * /*pathnames*/) {
  return IOStatus::NotSupported(
      "Single Object Path Purger does not support FindObsoleteFiles");
}
IOStatus CloudFileSystemImpl::FindObsoleteDbid(
    const std::string & /*bucket_name_prefix*/,
    std::vector<std::string> * /*to_delete_list*/) {
  return IOStatus::NotSupported(
      "Single Object Path Purger does not support FindObsoleteDbid");
}

IOStatus CloudFileSystemImpl::extractParents(
    const std::string & /*bucket_name_prefix*/, const DbidList & /*dbid_list*/,
    DbidParents * /*parents*/) {
  return IOStatus::NotSupported(
      "Single Object Path Purger does not support extractParents");
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE

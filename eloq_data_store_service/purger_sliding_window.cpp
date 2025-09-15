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

#include <glog/logging.h>
#include <rocksdb/cloud/cloud_storage_provider.h>
#include <rocksdb/db.h>
#include <rocksdb/io_status.h>
#include <rocksdb/listener.h>

#include <chrono>
#include <condition_variable>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include "purger_sliding_window.h"

namespace EloqDS
{
using std::make_unique;

// S3FileNumberUpdater implementation

S3FileNumberUpdater::S3FileNumberUpdater(
    const std::string &bucket_name,
    const std::string &s3_object_path,
    std::shared_ptr<rocksdb::CloudStorageProvider> storage_provider)
    : bucket_name_(bucket_name),
      s3_object_path_(s3_object_path),
      storage_provider_(storage_provider)
{
}

void S3FileNumberUpdater::UpdateSmallestFileNumber(uint64_t file_number,
                                                   const std::string &epoch)
{
    std::string content = std::to_string(file_number);
    std::string object_key = GetS3ObjectKey(epoch);

    // Write to temp local file at first, then upload to S3
    std::string time_id = std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    std::string temp_file_path =
        "/tmp/smallest_file_number_" + epoch + "_" + time_id + ".txt";
    std::ofstream temp_file(temp_file_path);
    if (!temp_file.is_open())
    {
        LOG(ERROR) << "Failed to open temp file for writing: "
                   << temp_file_path;
        return;
    }
    temp_file << content;
    temp_file.close();
    // Now upload the temp file to S3

    rocksdb::IOStatus s = storage_provider_->PutCloudObject(
        temp_file_path, bucket_name_, object_key);

    if (!s.ok())
    {
        LOG(ERROR) << "Failed to update smallest file number to S3: "
                   << s.ToString() << " ,bucket_name: " << bucket_name_
                   << ", object_key: " << object_key
                   << ", file_number: " << file_number;
    }
    else
    {
        DLOG(INFO) << "Updated smallest file number in S3: "
                   << " bucket_name: " << bucket_name_
                   << ", file_number: " << file_number
                   << ", object_key: " << object_key;
    }

    // Remove the temp file
    if (std::remove(temp_file_path.c_str()) != 0)
    {
        LOG(WARNING) << "Failed to remove temp file: " << temp_file_path;
    }
}

uint64_t S3FileNumberUpdater::ReadSmallestFileNumber(const std::string &epoch)
{
    std::string object_key = GetS3ObjectKey(epoch);
    std::string content;

    rocksdb::IOStatus s =
        storage_provider_->GetCloudObject(bucket_name_, object_key, content);

    if (!s.ok())
    {
        DLOG(INFO) << "Failed to read smallest file number from S3: "
                   << s.ToString() << ", object_key: " << object_key
                   << ", returning UINT64_MIN";
        return std::numeric_limits<uint64_t>::min();
    }

    try
    {
        uint64_t file_number = std::stoull(content);
        DLOG(INFO) << "Read smallest file number from S3: " << file_number
                   << ", object_key: " << object_key;
        return file_number;
    }
    catch (const std::exception &e)
    {
        LOG(ERROR) << "Failed to parse smallest file number from S3 content: '"
                   << content << "', error: " << e.what()
                   << ", object_key: " << object_key;
        return std::numeric_limits<uint64_t>::min();
    }
}

void S3FileNumberUpdater::WriteNoActivityMarker(const std::string &epoch)
{
    UpdateSmallestFileNumber(std::numeric_limits<uint64_t>::max(), epoch);
}

std::string S3FileNumberUpdater::GetS3ObjectKey(const std::string &epoch) const
{
    std::ostringstream oss;
    oss << s3_object_path_;
    if (!s3_object_path_.empty() && s3_object_path_.back() != '/')
    {
        oss << "/";
    }
    oss << "smallest_new_file_number-" << epoch;
    return oss.str();
}

// SlidingWindow implementation

SlidingWindow::SlidingWindow(
    std::chrono::milliseconds window_duration,
    std::chrono::milliseconds s3_update_interval,
    const std::string &epoch,
    const std::string &bucket_name,
    const std::string &s3_object_path,
    std::shared_ptr<rocksdb::CloudStorageProvider> storage_provider)
    : window_duration_(window_duration),
      s3_update_interval_(s3_update_interval),
      epoch_(epoch),
      should_stop_(false)
{
    s3_updater_ = std::make_unique<S3FileNumberUpdater>(
        bucket_name, s3_object_path, storage_provider);

    // Start the timer thread
    timer_thread_ = make_unique<std::thread>(&SlidingWindow::TimerWorker, this);

    DLOG(INFO) << "SlidingWindow started for epoch " << epoch_
               << ", window_duration: " << window_duration_.count() << "ms"
               << ", s3_update_interval: " << s3_update_interval_.count()
               << "ms";
}

SlidingWindow::~SlidingWindow()
{
    Stop();
}

void SlidingWindow::SetEpoch(const std::string &epoch)
{
    std::lock_guard<std::mutex> lock(window_mutex_);
    epoch_ = epoch;
}

std::string SlidingWindow::GetEpoch()
{
    std::lock_guard<std::mutex> lock(window_mutex_);
    return epoch_;
}

void SlidingWindow::AddFileNumber(uint64_t file_number,
                                  int thread_id,
                                  uint64_t job_id)
{
    std::lock_guard<std::mutex> lock(window_mutex_);

    std::string key = GenerateKey(thread_id, job_id);
    window_entries_.emplace(key, WindowEntry(file_number));

    DLOG(INFO) << "Added file number to sliding window: " << file_number
               << ", thread_id: " << thread_id << ", job_id: " << job_id
               << ", epoch: " << epoch_
               << ", window size: " << window_entries_.size();
}

void SlidingWindow::RemoveFileNumber(int thread_id, uint64_t job_id)
{
    std::lock_guard<std::mutex> lock(window_mutex_);

    std::string key = GenerateKey(thread_id, job_id);
    auto it = window_entries_.find(key);

    if (it != window_entries_.end())
    {
        uint64_t removed_file_number = it->second.file_number;
        window_entries_.erase(it);

        DLOG(INFO) << "Removed file number from sliding window: "
                   << removed_file_number << ", thread_id: " << thread_id
                   << ", job_id: " << job_id << ", epoch: " << epoch_
                   << ", window size: " << window_entries_.size();
    }
    else
    {
        DLOG(WARNING)
            << "Attempted to remove non-existent entry from sliding window: "
            << "thread_id: " << thread_id << ", job_id: " << job_id
            << ", epoch: " << epoch_;
    }
}

uint64_t SlidingWindow::GetSmallestFileNumber()
{
    std::lock_guard<std::mutex> lock(window_mutex_);

    if (window_entries_.empty())
    {
        return std::numeric_limits<uint64_t>::max();
    }

    uint64_t smallest = std::numeric_limits<uint64_t>::max();
    for (const auto &entry : window_entries_)
    {
        if (entry.second.file_number < smallest)
        {
            smallest = entry.second.file_number;
        }
    }

    DLOG(INFO) << "Current smallest file number: " << smallest
               << ", epoch: " << epoch_
               << ", window size: " << window_entries_.size();

    return smallest;
}

void SlidingWindow::Stop()
{
    // Signal the timer thread to stop
    {
        std::lock_guard<std::mutex> lock(window_mutex_);
        if (should_stop_)
        {
            return;  // Already stopped
        }
        should_stop_ = true;
    }
    cv_.notify_all();

    // Wait for the timer thread to finish
    if (timer_thread_ && timer_thread_->joinable())
    {
        timer_thread_->join();
        timer_thread_.reset();
    }

    DLOG(INFO) << "SlidingWindow stopped for epoch " << epoch_;
}

void SlidingWindow::TimerWorker()
{
    std::unique_lock<std::mutex> lock(window_mutex_);

    while (!should_stop_)
    {
        DLOG(INFO) << "SlidingWindow timer tick for epoch " << epoch_;
        // Wait for the specified interval or stop signal
        cv_.wait_for(
            lock, s3_update_interval_, [this] { return should_stop_; });

        if (should_stop_)
        {
            break;
        }

        // do not attempt S3 update if epoch is empty (indicates epoch is still
        // being set)
        if (epoch_.empty())
        {
            continue;
        }
        DLOG(INFO) << "SlidingWindow timer processing for epoch " << epoch_;

        // Release lock during S3 operation to avoid blocking AddFileNumber
        lock.unlock();
        FlushToS3();
        lock.lock();
    }

    DLOG(INFO) << "SlidingWindow timer thread exiting for epoch " << epoch_;
}

void SlidingWindow::FlushToS3()
{
    uint64_t smallest = GetSmallestFileNumber();

    if (smallest == std::numeric_limits<uint64_t>::max())
    {
        // No activity marker
        s3_updater_->WriteNoActivityMarker(epoch_);
        DLOG(INFO) << "Wrote no activity marker to S3 for epoch " << epoch_;
    }
    else
    {
        s3_updater_->UpdateSmallestFileNumber(smallest, epoch_);
        DLOG(INFO) << "Updated S3 with smallest file number: " << smallest
                   << ", epoch: " << epoch_;
    }
}

std::string SlidingWindow::GenerateKey(int thread_id, uint64_t job_id) const
{
    std::ostringstream oss;
    oss << thread_id << "-" << job_id;
    return oss.str();
}

}  // namespace EloqDS

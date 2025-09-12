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

#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <rocksdb/cloud/cloud_storage_provider.h>
#include <glog/logging.h>

namespace EloqDS
{

/**
 * @brief S3 file updater for writing smallest file number to S3
 */
class S3FileNumberUpdater
{
public:
    S3FileNumberUpdater(const std::string& bucket_name,
                       const std::string& s3_object_path,
                       const std::string& epoch,
                       std::shared_ptr<rocksdb::CloudStorageProvider> storage_provider);

    ~S3FileNumberUpdater() = default;

    /**
     * @brief Update the smallest file number in S3
     * @param file_number The smallest file number to write
     */
    void UpdateSmallestFileNumber(uint64_t file_number);

    /**
     * @brief Read the smallest file number from S3
     * @return The smallest file number, or UINT64_MAX if not found
     */
    uint64_t ReadSmallestFileNumber();

    /**
     * @brief Write no activity marker (UINT64_MAX) to S3
     */
    void WriteNoActivityMarker();

private:
    std::string bucket_name_;
    std::string s3_object_path_;
    std::string epoch_;
    std::shared_ptr<rocksdb::CloudStorageProvider> storage_provider_;

    std::string GetS3ObjectKey() const;
};

/**
 * @brief Time-based sliding window for tracking file numbers with automatic S3 updates
 */
class SlidingWindow
{
public:
    /**
     * @brief Constructor for sliding window
     * @param window_duration Duration to keep entries in the window
     * @param s3_update_interval Interval for updating S3 file
     * @param epoch The epoch string for this DB instance
     * @param bucket_name S3 bucket name
     * @param s3_object_path S3 object path
     * @param storage_provider Cloud storage provider for S3 operations
     */
    SlidingWindow(std::chrono::milliseconds window_duration,
                  std::chrono::milliseconds s3_update_interval,
                  const std::string& epoch,
                  const std::string& bucket_name,
                  const std::string& s3_object_path,
                  std::shared_ptr<rocksdb::CloudStorageProvider> storage_provider);

    /**
     * @brief Destructor - stops the timer thread
     */
    ~SlidingWindow();

    /**
     * @brief Add a file number to the sliding window
     * @param file_number The file number to add
     */
    void AddFileNumber(uint64_t file_number);

    /**
     * @brief Get the smallest file number in the current window
     * @return The smallest file number, or UINT64_MAX if window is empty
     */
    uint64_t GetSmallestFileNumber();

    /**
     * @brief Stop the sliding window and cleanup
     */
    void Stop();

private:
    struct WindowEntry
    {
        uint64_t file_number;
        std::chrono::steady_clock::time_point timestamp;

        WindowEntry(uint64_t num)
            : file_number(num), timestamp(std::chrono::steady_clock::now()) {}
    };

    std::deque<WindowEntry> window_entries_;
    std::chrono::milliseconds window_duration_;
    std::chrono::milliseconds s3_update_interval_;
    std::string epoch_;

    std::unique_ptr<S3FileNumberUpdater> s3_updater_;

    // Threading
    std::unique_ptr<std::thread> timer_thread_;
    std::mutex window_mutex_;
    std::condition_variable cv_;
    bool should_stop_;

    /**
     * @brief Timer thread worker function
     */
    void TimerWorker();

    /**
     * @brief Flush current minimum file number to S3
     */
    void FlushToS3();

    /**
     * @brief Remove expired entries from the window
     */
    void CleanupExpiredEntries();
};

} // namespace EloqDS


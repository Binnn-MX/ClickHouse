#pragma once
#include <config.h>

#if USE_PAIMON_CPP

#include <memory>
#include <string>
#include <vector>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/ReadSettings.h>
#include <Interpreters/Context_fwd.h>

#include <paimon/fs/file_system.h>

namespace DB
{

class CHInputStream final : public paimon::InputStream
{
public:
    CHInputStream(
        ObjectStoragePtr object_storage_,
        const std::string & path_,
        const ReadSettings & read_settings_);

    paimon::Status Seek(int64_t offset, paimon::SeekOrigin origin) override;
    paimon::Result<int64_t> GetPos() const override;
    paimon::Result<int32_t> Read(char * buffer, uint32_t size) override;
    paimon::Result<int32_t> Read(char * buffer, uint32_t size, uint64_t offset) override;
    void ReadAsync(char * buffer, uint32_t size, uint64_t offset, std::function<void(paimon::Status)> && callback) override;
    paimon::Result<std::string> GetUri() const override;
    paimon::Result<uint64_t> Length() const override;
    paimon::Status Close() override;

private:
    void ensureBuffer();

    ObjectStoragePtr object_storage;
    std::string path;
    ReadSettings read_settings;
    std::unique_ptr<ReadBufferFromFileBase> buffer;
    int64_t position = 0;
    std::optional<uint64_t> file_size;
};

class CHOutputStream final : public paimon::OutputStream
{
public:
    CHOutputStream(
        ObjectStoragePtr object_storage_,
        const std::string & path_,
        bool overwrite_);

    paimon::Result<int32_t> Write(const char * buffer, uint32_t size) override;
    paimon::Status Flush() override;
    paimon::Result<int64_t> GetPos() const override;
    paimon::Result<std::string> GetUri() const override;
    paimon::Status Close() override;

private:
    ObjectStoragePtr object_storage;
    std::string path;
    std::unique_ptr<WriteBufferFromFileBase> buffer;
    int64_t position = 0;
};

class CHFileStatus final : public paimon::FileStatus
{
public:
    CHFileStatus(std::string path_, uint64_t size_, bool is_dir_, int64_t modification_time_)
        : path(std::move(path_)), size(size_), is_dir(is_dir_), modification_time(modification_time_)
    {
    }

    uint64_t GetLen() const override { return size; }
    bool IsDir() const override { return is_dir; }
    std::string GetPath() const override { return path; }
    int64_t GetModificationTime() const override { return modification_time; }

private:
    std::string path;
    uint64_t size;
    bool is_dir;
    int64_t modification_time;
};

class CHBasicFileStatus final : public paimon::BasicFileStatus
{
public:
    CHBasicFileStatus(std::string path_, bool is_dir_)
        : path(std::move(path_)), is_dir(is_dir_)
    {
    }

    bool IsDir() const override { return is_dir; }
    std::string GetPath() const override { return path; }

private:
    std::string path;
    bool is_dir;
};

class CHFileSystem final : public paimon::FileSystem
{
public:
    CHFileSystem(ObjectStoragePtr object_storage_, ReadSettings read_settings_);

    paimon::Result<std::unique_ptr<paimon::InputStream>> Open(const std::string & path) const override;
    paimon::Result<std::unique_ptr<paimon::OutputStream>> Create(const std::string & path, bool overwrite) const override;
    paimon::Status Mkdirs(const std::string & path) const override;
    paimon::Status Rename(const std::string & src, const std::string & dst) const override;
    paimon::Status Delete(const std::string & path, bool recursive) const override;
    paimon::Result<std::unique_ptr<paimon::FileStatus>> GetFileStatus(const std::string & path) const override;
    paimon::Status ListDir(
        const std::string & directory,
        std::vector<std::unique_ptr<paimon::BasicFileStatus>> * file_status_list) const override;
    paimon::Status ListFileStatus(
        const std::string & path,
        std::vector<std::unique_ptr<paimon::FileStatus>> * file_status_list) const override;
    paimon::Result<bool> Exists(const std::string & path) const override;

private:
    ObjectStoragePtr object_storage;
    ReadSettings read_settings;
};

std::shared_ptr<paimon::FileSystem> createPaimonFileSystem(
    ObjectStoragePtr object_storage,
    const ContextPtr & context);

}

#endif

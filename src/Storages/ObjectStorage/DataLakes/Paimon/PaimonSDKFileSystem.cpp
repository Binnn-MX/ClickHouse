#include <config.h>

#if USE_PAIMON_CPP

#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonSDKFileSystem.h>

#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteSettings.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

// --- CHInputStream ---

CHInputStream::CHInputStream(
    ObjectStoragePtr object_storage_,
    const std::string & path_,
    const ReadSettings & read_settings_)
    : object_storage(std::move(object_storage_))
    , path(path_)
    , read_settings(read_settings_)
{
}

void CHInputStream::ensureBuffer()
{
    if (!buffer)
    {
        StoredObject stored_object(path);
        buffer = object_storage->readObject(stored_object, read_settings);
    }
}

paimon::Status CHInputStream::Seek(int64_t offset, paimon::SeekOrigin origin)
{
    try
    {
        ensureBuffer();

        off_t absolute_offset = 0;
        switch (origin)
        {
            case paimon::FS_SEEK_SET:
                absolute_offset = offset;
                break;
            case paimon::FS_SEEK_CUR:
                absolute_offset = position + offset;
                break;
            case paimon::FS_SEEK_END:
            {
                auto len_result = Length();
                if (!len_result.ok())
                    return len_result.status();
                absolute_offset = static_cast<int64_t>(len_result.value()) + offset;
                break;
            }
        }

        buffer->seek(absolute_offset, SEEK_SET);
        position = absolute_offset;
        return paimon::Status::OK();
    }
    catch (const Exception & e)
    {
        return paimon::Status::IOError("Seek failed: ", e.message());
    }
}

paimon::Result<int64_t> CHInputStream::GetPos() const
{
    return position;
}

paimon::Result<int32_t> CHInputStream::Read(char * buf, uint32_t size)
{
    try
    {
        ensureBuffer();
        size_t bytes_read = buffer->read(buf, size);
        position += static_cast<int64_t>(bytes_read);
        return static_cast<int32_t>(bytes_read);
    }
    catch (const Exception & e)
    {
        return paimon::Status::IOError("Read failed: ", e.message());
    }
}

paimon::Result<int32_t> CHInputStream::Read(char * buf, uint32_t size, uint64_t offset)
{
    try
    {
        ensureBuffer();
        auto old_pos = position;
        buffer->seek(static_cast<off_t>(offset), SEEK_SET);
        size_t bytes_read = buffer->read(buf, size);
        buffer->seek(old_pos, SEEK_SET);
        return static_cast<int32_t>(bytes_read);
    }
    catch (const Exception & e)
    {
        return paimon::Status::IOError("Positional read failed: ", e.message());
    }
}

void CHInputStream::ReadAsync(
    char * buf, uint32_t size, uint64_t offset,
    std::function<void(paimon::Status)> && callback)
{
    auto result = Read(buf, size, offset);
    if (result.ok())
        callback(paimon::Status::OK());
    else
        callback(result.status());
}

paimon::Result<std::string> CHInputStream::GetUri() const
{
    return path;
}

paimon::Result<uint64_t> CHInputStream::Length() const
{
    if (file_size.has_value())
        return file_size.value();

    try
    {
        StoredObject stored_object(path);
        auto metadata = object_storage->getObjectMetadata(path, /*with_tags=*/false);
        const_cast<CHInputStream *>(this)->file_size = metadata.size_bytes;
        return metadata.size_bytes;
    }
    catch (const Exception & e)
    {
        return paimon::Status::IOError("Failed to get file length: ", e.message());
    }
}

paimon::Status CHInputStream::Close()
{
    buffer.reset();
    return paimon::Status::OK();
}

// --- CHOutputStream ---

CHOutputStream::CHOutputStream(
    ObjectStoragePtr object_storage_,
    const std::string & path_,
    bool /* overwrite */)
    : object_storage(std::move(object_storage_))
    , path(path_)
{
    StoredObject stored_object(path);
    buffer = object_storage->writeObject(stored_object, WriteMode::Rewrite);
}

paimon::Result<int32_t> CHOutputStream::Write(const char * buf, uint32_t size)
{
    try
    {
        buffer->write(buf, size);
        position += size;
        return static_cast<int32_t>(size);
    }
    catch (const Exception & e)
    {
        return paimon::Status::IOError("Write failed: ", e.message());
    }
}

paimon::Status CHOutputStream::Flush()
{
    try
    {
        buffer->sync();
        return paimon::Status::OK();
    }
    catch (const Exception & e)
    {
        return paimon::Status::IOError("Flush failed: ", e.message());
    }
}

paimon::Result<int64_t> CHOutputStream::GetPos() const
{
    return position;
}

paimon::Result<std::string> CHOutputStream::GetUri() const
{
    return path;
}

paimon::Status CHOutputStream::Close()
{
    try
    {
        if (buffer)
        {
            buffer->finalize();
            buffer.reset();
        }
        return paimon::Status::OK();
    }
    catch (const Exception & e)
    {
        return paimon::Status::IOError("Close failed: ", e.message());
    }
}

// --- CHFileSystem ---

CHFileSystem::CHFileSystem(ObjectStoragePtr object_storage_, ReadSettings read_settings_)
    : object_storage(std::move(object_storage_))
    , read_settings(std::move(read_settings_))
{
}

paimon::Result<std::unique_ptr<paimon::InputStream>> CHFileSystem::Open(const std::string & path) const
{
    try
    {
        StoredObject stored_object(path);
        if (!object_storage->exists(stored_object))
            return paimon::Status::NotExist("File not found: ", path);

        return std::make_unique<CHInputStream>(object_storage, path, read_settings);
    }
    catch (const Exception & e)
    {
        return paimon::Status::IOError("Open failed: ", e.message());
    }
}

paimon::Result<std::unique_ptr<paimon::OutputStream>> CHFileSystem::Create(const std::string & path, bool overwrite) const
{
    try
    {
        return std::make_unique<CHOutputStream>(object_storage, path, overwrite);
    }
    catch (const Exception & e)
    {
        return paimon::Status::IOError("Create failed: ", e.message());
    }
}

paimon::Status CHFileSystem::Mkdirs(const std::string & /* path */) const
{
    return paimon::Status::OK();
}

paimon::Status CHFileSystem::Rename(const std::string & src, const std::string & dst) const
{
    try
    {
        StoredObject src_object(src);
        StoredObject dst_object(dst);
        object_storage->copyObject(src_object, dst_object, read_settings, WriteSettings{});
        object_storage->removeObjectIfExists(src_object);
        return paimon::Status::OK();
    }
    catch (const Exception & e)
    {
        return paimon::Status::IOError("Rename failed: ", e.message());
    }
}

paimon::Status CHFileSystem::Delete(const std::string & path, bool /* recursive */) const
{
    try
    {
        StoredObject stored_object(path);
        object_storage->removeObjectIfExists(stored_object);
        return paimon::Status::OK();
    }
    catch (const Exception & e)
    {
        return paimon::Status::IOError("Delete failed: ", e.message());
    }
}

paimon::Result<std::unique_ptr<paimon::FileStatus>> CHFileSystem::GetFileStatus(const std::string & path) const
{
    try
    {
        auto metadata = object_storage->getObjectMetadata(path, /*with_tags=*/false);
        auto status = std::make_unique<CHFileStatus>(
            path,
            metadata.size_bytes,
            /*is_dir=*/false,
            metadata.last_modified.epochMicroseconds() / 1000);
        return status;
    }
    catch (const Exception & e)
    {
        return paimon::Status::IOError("GetFileStatus failed: ", e.message());
    }
}

paimon::Status CHFileSystem::ListDir(
    const std::string & directory,
    std::vector<std::unique_ptr<paimon::BasicFileStatus>> * file_status_list) const
{
    try
    {
        RelativePathsWithMetadata children;
        object_storage->listObjects(directory, children, /*max_keys=*/0);
        for (const auto & child : children)
        {
            bool is_dir = child->getPath().ends_with("/");
            file_status_list->push_back(
                std::make_unique<CHBasicFileStatus>(child->getPath(), is_dir));
        }
        return paimon::Status::OK();
    }
    catch (const Exception & e)
    {
        return paimon::Status::IOError("ListDir failed: ", e.message());
    }
}

paimon::Status CHFileSystem::ListFileStatus(
    const std::string & path,
    std::vector<std::unique_ptr<paimon::FileStatus>> * file_status_list) const
{
    try
    {
        RelativePathsWithMetadata children;
        object_storage->listObjects(path, children, /*max_keys=*/0);
        for (const auto & child : children)
        {
            bool is_dir = child->getPath().ends_with("/");
            uint64_t size = child->metadata.has_value() ? child->metadata->size_bytes : 0;
            int64_t mod_time = child->metadata.has_value()
                ? child->metadata->last_modified.epochMicroseconds() / 1000
                : 0;
            file_status_list->push_back(
                std::make_unique<CHFileStatus>(child->getPath(), size, is_dir, mod_time));
        }
        return paimon::Status::OK();
    }
    catch (const Exception & e)
    {
        return paimon::Status::IOError("ListFileStatus failed: ", e.message());
    }
}

paimon::Result<bool> CHFileSystem::Exists(const std::string & path) const
{
    try
    {
        StoredObject stored_object(path);
        return object_storage->exists(stored_object);
    }
    catch (const Exception & e)
    {
        return paimon::Status::IOError("Exists check failed: ", e.message());
    }
}

std::shared_ptr<paimon::FileSystem> createPaimonFileSystem(
    ObjectStoragePtr object_storage,
    const ContextPtr & context)
{
    ReadSettings read_settings = context->getReadSettings();
    return std::make_shared<CHFileSystem>(std::move(object_storage), std::move(read_settings));
}

}

#endif

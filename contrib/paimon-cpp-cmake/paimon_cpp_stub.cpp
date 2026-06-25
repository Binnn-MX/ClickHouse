// Stub implementation of the paimon-cpp SDK symbols that ClickHouse links
// against. Every entry point returns Status::NotImplemented or a benign
// default; the wrapper code in src/Storages/ObjectStorage/DataLakes/Paimon/*
// converts those into LOGICAL_ERROR exceptions. This keeps the build
// link-clean while the real upstream SDK is not yet buildable in ClickHouse's
// contrib tree.
//
// As soon as upstream paimon-cpp ships a usable CMake build (or
// PAIMON_CPP_LINK_MODE=prebuilt is set), this file should be excluded from the
// build via contrib/paimon-cpp-cmake/CMakeLists.txt and the real symbols will
// take over.

#include <paimon/defs.h>
#include <paimon/fs/file_system.h>
#include <paimon/predicate/literal.h>
#include <paimon/predicate/predicate.h>
#include <paimon/predicate/predicate_builder.h>
#include <paimon/read_context.h>
#include <paimon/reader/batch_reader.h>
#include <paimon/scan_context.h>
#include <paimon/status.h>
#include <paimon/table/source/plan.h>
#include <paimon/table/source/split.h>
#include <paimon/table/source/table_read.h>
#include <paimon/table/source/table_scan.h>

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace paimon
{

namespace
{
constexpr const char * kStubMessage =
    "paimon-cpp SDK is not linked into this ClickHouse build (stub mode). "
    "Either rebuild with PAIMON_CPP_LINK_MODE=prebuilt and PAIMON_CPP_PREBUILT_LIBRARY "
    "or wait until the upstream SDK provides a buildable CMake target.";
}

// ---------------------------------------------------------------------------
// Status: out-of-line members.  The remaining methods are inline in
// paimon/status.h.  `Status::State` is a private nested struct defined in the
// header; we simply construct it via aggregate initialization.
// ---------------------------------------------------------------------------

Status::Status(StatusCode code, const std::string & msg)
    : Status(code, std::string(msg), nullptr)
{
}

Status::Status(StatusCode code, std::string msg, std::shared_ptr<StatusDetail> detail)
{
    state_ = new State{code, std::move(msg), std::move(detail)};
}

void Status::CopyFrom(const Status & s)
{
    delete state_;
    state_ = (s.state_ == nullptr) ? nullptr : new State(*s.state_);
}

std::string Status::CodeAsString() const
{
    return CodeAsString(code());
}

std::string Status::CodeAsString(StatusCode code)
{
    switch (code)
    {
        case StatusCode::OK: return "OK";
        case StatusCode::OutOfMemory: return "OutOfMemory";
        case StatusCode::KeyError: return "KeyError";
        case StatusCode::TypeError: return "TypeError";
        case StatusCode::Invalid: return "Invalid";
        case StatusCode::IOError: return "IOError";
        case StatusCode::CapacityError: return "CapacityError";
        case StatusCode::IndexError: return "IndexError";
        case StatusCode::Cancelled: return "Cancelled";
        case StatusCode::UnknownError: return "UnknownError";
        case StatusCode::NotImplemented: return "NotImplemented";
        case StatusCode::SerializationError: return "SerializationError";
        case StatusCode::NotExist: return "NotExist";
        case StatusCode::Exist: return "Exist";
    }
    return "Unknown";
}

std::string Status::ToString() const
{
    if (state_ == nullptr)
        return "OK";
    std::string out = CodeAsString();
    if (!state_->msg.empty())
    {
        out += ": ";
        out += state_->msg;
    }
    return out;
}

[[noreturn]] void Status::Abort(const std::string & /*message*/) const
{
    std::abort();
}
[[noreturn]] void Status::Abort() const
{
    std::abort();
}

// ---------------------------------------------------------------------------
// FileSystem: out-of-line destructor + non-pure-virtual helpers.
// ---------------------------------------------------------------------------

FileSystem::~FileSystem() = default;

Result<bool> FileSystem::IsObjectStore(const std::string & /*path_str*/)
{
    return Status::NotImplemented(kStubMessage);
}

Status FileSystem::ReadFile(const std::string & /*path*/, std::string * /*content*/)
{
    return Status::NotImplemented(kStubMessage);
}

Status FileSystem::WriteFile(const std::string & /*path*/, const std::string & /*content*/, bool /*overwrite*/)
{
    return Status::NotImplemented(kStubMessage);
}

Status FileSystem::AtomicStore(const std::string & /*path*/, const std::string & /*content*/)
{
    return Status::NotImplemented(kStubMessage);
}

// ---------------------------------------------------------------------------
// Literal: PIMPL destructor + the constructors/operators ClickHouse uses.
// ---------------------------------------------------------------------------

class Literal::Impl
{
public:
    Impl() = default;
    Impl(const Impl &) = default;
    Impl(Impl &&) noexcept = default;
    Impl & operator=(const Impl &) = default;
    Impl & operator=(Impl &&) noexcept = default;
};

Literal::Literal(FieldType /*type*/) : impl_(std::make_unique<Impl>()) {}
Literal::Literal(FieldType /*binary_type*/, const char * /*str*/, size_t /*size*/)
    : impl_(std::make_unique<Impl>()) {}
Literal::Literal(FieldType /*binary_type*/, const char * /*str*/, size_t /*size*/, bool /*own_data*/)
    : impl_(std::make_unique<Impl>()) {}
Literal::Literal(FieldType /*date_type*/, int32_t /*date_value*/) : impl_(std::make_unique<Impl>()) {}
Literal::Literal(const Literal & other)
    : impl_(other.impl_ ? std::make_unique<Impl>(*other.impl_) : nullptr) {}
Literal::Literal(Literal && other) : impl_(std::move(other.impl_)) {}
Literal & Literal::operator=(const Literal & other)
{
    if (this != &other)
        impl_ = other.impl_ ? std::make_unique<Impl>(*other.impl_) : nullptr;
    return *this;
}
Literal & Literal::operator=(Literal && other)
{
    impl_ = std::move(other.impl_);
    return *this;
}
Literal::~Literal() = default;

bool Literal::operator==(const Literal & other) const
{
    // Stub: only treat self-comparison and the "both null literals" case as
    // equal so the basic reflexivity contract used by unordered containers
    // holds. Cross-instance comparisons cannot be answered without the real
    // SDK and conservatively report inequality.
    if (this == &other)
        return true;
    return impl_ == nullptr && other.impl_ == nullptr;
}
bool Literal::operator!=(const Literal & other) const { return !(*this == other); }
bool Literal::IsNull() const { return false; }
FieldType Literal::GetType() const { return FieldType::UNKNOWN; }
std::string Literal::ToString() const { return "<paimon-cpp-stub:Literal>"; }
size_t Literal::HashCode() const { return 0; }
Result<int32_t> Literal::CompareTo(const Literal & /*other*/) const
{
    return Status::NotImplemented(kStubMessage);
}

// Explicit instantiations matching the constructors invoked by ClickHouse.
template <> Literal::Literal(const bool &)    : impl_(std::make_unique<Impl>()) {}
template <> Literal::Literal(const int8_t &)  : impl_(std::make_unique<Impl>()) {}
template <> Literal::Literal(const int16_t &) : impl_(std::make_unique<Impl>()) {}
template <> Literal::Literal(const int32_t &) : impl_(std::make_unique<Impl>()) {}
template <> Literal::Literal(const int64_t &) : impl_(std::make_unique<Impl>()) {}
template <> Literal::Literal(const float &)   : impl_(std::make_unique<Impl>()) {}
template <> Literal::Literal(const double &)  : impl_(std::make_unique<Impl>()) {}

// ---------------------------------------------------------------------------
// PredicateBuilder: every entry point returns nullptr or NotImplemented.
// ---------------------------------------------------------------------------

std::shared_ptr<Predicate> PredicateBuilder::Equal(int32_t, const std::string &, const FieldType &, const Literal &)
{
    return nullptr;
}
std::shared_ptr<Predicate> PredicateBuilder::NotEqual(int32_t, const std::string &, const FieldType &, const Literal &)
{
    return nullptr;
}
std::shared_ptr<Predicate> PredicateBuilder::LessThan(int32_t, const std::string &, const FieldType &, const Literal &)
{
    return nullptr;
}
std::shared_ptr<Predicate> PredicateBuilder::LessOrEqual(int32_t, const std::string &, const FieldType &, const Literal &)
{
    return nullptr;
}
std::shared_ptr<Predicate> PredicateBuilder::GreaterThan(int32_t, const std::string &, const FieldType &, const Literal &)
{
    return nullptr;
}
std::shared_ptr<Predicate> PredicateBuilder::GreaterOrEqual(int32_t, const std::string &, const FieldType &, const Literal &)
{
    return nullptr;
}
std::shared_ptr<Predicate> PredicateBuilder::IsNull(int32_t, const std::string &, const FieldType &)
{
    return nullptr;
}
std::shared_ptr<Predicate> PredicateBuilder::IsNotNull(int32_t, const std::string &, const FieldType &)
{
    return nullptr;
}
std::shared_ptr<Predicate> PredicateBuilder::In(int32_t, const std::string &, const FieldType &, const std::vector<Literal> &)
{
    return nullptr;
}
std::shared_ptr<Predicate> PredicateBuilder::NotIn(int32_t, const std::string &, const FieldType &, const std::vector<Literal> &)
{
    return nullptr;
}
std::shared_ptr<Predicate> PredicateBuilder::Between(
    int32_t, const std::string &, const FieldType &, const Literal &, const Literal &)
{
    return nullptr;
}
Result<std::shared_ptr<Predicate>> PredicateBuilder::And(const std::vector<std::shared_ptr<Predicate>> &)
{
    return Status::NotImplemented(kStubMessage);
}
Result<std::shared_ptr<Predicate>> PredicateBuilder::Or(const std::vector<std::shared_ptr<Predicate>> &)
{
    return Status::NotImplemented(kStubMessage);
}
Result<std::shared_ptr<Predicate>> PredicateBuilder::Not(const std::shared_ptr<Predicate> &)
{
    return Status::NotImplemented(kStubMessage);
}
Result<std::shared_ptr<Predicate>> PredicateBuilder::StartsWith(int32_t, const std::string &, const FieldType &, const Literal &)
{
    return Status::NotImplemented(kStubMessage);
}
Result<std::shared_ptr<Predicate>> PredicateBuilder::EndsWith(int32_t, const std::string &, const FieldType &, const Literal &)
{
    return Status::NotImplemented(kStubMessage);
}
Result<std::shared_ptr<Predicate>> PredicateBuilder::Contains(int32_t, const std::string &, const FieldType &, const Literal &)
{
    return Status::NotImplemented(kStubMessage);
}
Result<std::shared_ptr<Predicate>> PredicateBuilder::Like(int32_t, const std::string &, const FieldType &, const Literal &)
{
    return Status::NotImplemented(kStubMessage);
}

// ---------------------------------------------------------------------------
// ScanContextBuilder / ReadContextBuilder: PIMPL.  Builders are no-ops that
// always fail at Finish().
// ---------------------------------------------------------------------------

class ScanContextBuilder::Impl {};
class ReadContextBuilder::Impl {};

ScanContextBuilder::ScanContextBuilder(const std::string & /*path*/) : impl_(std::make_unique<Impl>()) {}
ScanContextBuilder::~ScanContextBuilder() = default;

ScanContextBuilder & ScanContextBuilder::SetLimit(int32_t) { return *this; }
ScanContextBuilder & ScanContextBuilder::SetBucketFilter(int32_t) { return *this; }
ScanContextBuilder & ScanContextBuilder::SetPartitionFilter(const std::vector<std::map<std::string, std::string>> &) { return *this; }
ScanContextBuilder & ScanContextBuilder::SetPredicate(const std::shared_ptr<Predicate> &) { return *this; }
ScanContextBuilder & ScanContextBuilder::SetGlobalIndexResult(const std::shared_ptr<GlobalIndexResult> &) { return *this; }
ScanContextBuilder & ScanContextBuilder::AddOption(const std::string &, const std::string &) { return *this; }
ScanContextBuilder & ScanContextBuilder::SetOptions(const std::map<std::string, std::string> &) { return *this; }
ScanContextBuilder & ScanContextBuilder::WithStreamingMode(bool) { return *this; }
ScanContextBuilder & ScanContextBuilder::WithMemoryPool(const std::shared_ptr<MemoryPool> &) { return *this; }
ScanContextBuilder & ScanContextBuilder::WithExecutor(const std::shared_ptr<Executor> &) { return *this; }
ScanContextBuilder & ScanContextBuilder::WithFileSystem(const std::shared_ptr<FileSystem> &) { return *this; }

Result<std::unique_ptr<ScanContext>> ScanContextBuilder::Finish()
{
    return Status::NotImplemented(kStubMessage);
}

ReadContextBuilder::ReadContextBuilder(const std::string & /*path*/) : impl_(std::make_unique<Impl>()) {}
ReadContextBuilder::~ReadContextBuilder() = default;
ReadContextBuilder::ReadContextBuilder(ReadContextBuilder &&) noexcept = default;
ReadContextBuilder & ReadContextBuilder::operator=(ReadContextBuilder &&) noexcept = default;

ReadContextBuilder & ReadContextBuilder::SetReadSchema(const std::vector<std::string> &) { return *this; }
ReadContextBuilder & ReadContextBuilder::SetReadFieldIds(const std::vector<int32_t> &) { return *this; }
ReadContextBuilder & ReadContextBuilder::SetOptions(const std::map<std::string, std::string> &) { return *this; }
ReadContextBuilder & ReadContextBuilder::AddOption(const std::string &, const std::string &) { return *this; }
ReadContextBuilder & ReadContextBuilder::SetPredicate(const std::shared_ptr<Predicate> &) { return *this; }
ReadContextBuilder & ReadContextBuilder::EnablePredicateFilter(bool) { return *this; }
ReadContextBuilder & ReadContextBuilder::EnablePrefetch(bool) { return *this; }
ReadContextBuilder & ReadContextBuilder::SetPrefetchCacheMode(PrefetchCacheMode) { return *this; }
ReadContextBuilder & ReadContextBuilder::WithCacheConfig(const CacheConfig &) { return *this; }
ReadContextBuilder & ReadContextBuilder::SetPrefetchBatchCount(uint32_t) { return *this; }
ReadContextBuilder & ReadContextBuilder::SetPrefetchMaxParallelNum(uint32_t) { return *this; }
ReadContextBuilder & ReadContextBuilder::EnableMultiThreadRowToBatch(bool) { return *this; }
ReadContextBuilder & ReadContextBuilder::SetRowToBatchThreadNumber(uint32_t) { return *this; }
ReadContextBuilder & ReadContextBuilder::WithMemoryPool(const std::shared_ptr<MemoryPool> &) { return *this; }
ReadContextBuilder & ReadContextBuilder::WithExecutor(const std::shared_ptr<Executor> &) { return *this; }
ReadContextBuilder & ReadContextBuilder::SetTableSchema(const std::string &) { return *this; }
ReadContextBuilder & ReadContextBuilder::WithBranch(const std::string &) { return *this; }
ReadContextBuilder & ReadContextBuilder::WithFileSystemSchemeToIdentifierMap(const std::map<std::string, std::string> &) { return *this; }
ReadContextBuilder & ReadContextBuilder::WithFileSystem(const std::shared_ptr<FileSystem> &) { return *this; }

Result<std::unique_ptr<ReadContext>> ReadContextBuilder::Finish()
{
    return Status::NotImplemented(kStubMessage);
}

// ---------------------------------------------------------------------------
// TableScan / TableRead / BatchReader: factory + helpers.
// ---------------------------------------------------------------------------

Result<std::unique_ptr<TableScan>> TableScan::Create(std::unique_ptr<ScanContext> /*context*/)
{
    return Status::NotImplemented(kStubMessage);
}

TableRead::TableRead(const std::shared_ptr<MemoryPool> & memory_pool) : pool_(memory_pool) {}

Result<std::unique_ptr<TableRead>> TableRead::Create(std::unique_ptr<ReadContext> /*context*/)
{
    return Status::NotImplemented(kStubMessage);
}

Result<std::unique_ptr<BatchReader>> TableRead::CreateReader(const std::vector<std::shared_ptr<Split>> & /*splits*/)
{
    return Status::NotImplemented(kStubMessage);
}

bool BatchReader::IsEofBatch(const ReadBatch & batch)
{
    return batch.first == nullptr;
}
BatchReader::ReadBatch BatchReader::MakeEofBatch()
{
    return std::make_pair(std::unique_ptr<ArrowArray>(), std::unique_ptr<ArrowSchema>());
}

// ---------------------------------------------------------------------------
// Split: serialization helpers (not exercised today, but ClickHouse links for
// completeness).
// ---------------------------------------------------------------------------

Result<std::shared_ptr<Split>> Split::Deserialize(const char * /*buffer*/, size_t /*length*/, const std::shared_ptr<MemoryPool> &)
{
    return Status::NotImplemented(kStubMessage);
}
Result<std::string> Split::Serialize(const std::shared_ptr<Split> & /*split*/, const std::shared_ptr<MemoryPool> &)
{
    return Status::NotImplemented(kStubMessage);
}

}  // namespace paimon

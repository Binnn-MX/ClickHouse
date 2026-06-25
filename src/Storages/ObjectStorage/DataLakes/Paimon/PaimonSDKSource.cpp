#include <config.h>

#if USE_PAIMON_CPP && (USE_ARROW || USE_ORC || USE_PARQUET)

#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonSDKSource.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PaimonSDKSource::PaimonSDKSource(
    std::shared_ptr<paimon::BatchReader> reader_,
    std::shared_ptr<std::mutex> reader_mutex_,
    const Block & header_,
    const FormatSettings & format_settings_)
    : ISource(std::make_shared<const Block>(header_), false)
    , reader(std::move(reader_))
    , reader_mutex(std::move(reader_mutex_))
    , bridge(header_, format_settings_)
{
}

Chunk PaimonSDKSource::generate()
{
    std::lock_guard lock(*reader_mutex);

    auto result = reader->NextBatch();
    if (!result.ok())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "paimon-cpp SDK NextBatch failed: {}",
            result.status().ToString());

    auto & batch = std::move(result).value();
    if (paimon::BatchReader::IsEofBatch(batch))
        return {};

    return bridge.convertBatch(std::move(batch.first), std::move(batch.second));
}

}

#endif

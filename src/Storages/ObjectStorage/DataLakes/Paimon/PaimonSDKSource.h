#pragma once
#include <config.h>

#if USE_PAIMON_CPP && (USE_ARROW || USE_ORC || USE_PARQUET)

#include <memory>
#include <mutex>
#include <vector>

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/ISource.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonArrowBridge.h>

#include <paimon/reader/batch_reader.h>

namespace DB
{

class PaimonSDKSource final : public ISource
{
public:
    PaimonSDKSource(
        std::shared_ptr<paimon::BatchReader> reader_,
        std::shared_ptr<std::mutex> reader_mutex_,
        const Block & header_,
        const FormatSettings & format_settings_);

    String getName() const override { return "PaimonSDKSource"; }

protected:
    Chunk generate() override;

private:
    std::shared_ptr<paimon::BatchReader> reader;
    std::shared_ptr<std::mutex> reader_mutex;
    PaimonArrowBridge bridge;
};

}

#endif

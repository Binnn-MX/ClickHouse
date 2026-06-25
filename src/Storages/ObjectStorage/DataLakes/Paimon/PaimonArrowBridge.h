#pragma once
#include <config.h>

#if USE_PAIMON_CPP && (USE_ARROW || USE_ORC || USE_PARQUET)

#include <memory>
#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Chunk.h>

struct ArrowArray;
struct ArrowSchema;

namespace DB
{

enum class PaimonRowKind : int8_t
{
    INSERT = 0,
    UPDATE_BEFORE = 1,
    UPDATE_AFTER = 2,
    DELETE = 3,
};

class PaimonArrowBridge
{
public:
    PaimonArrowBridge(const Block & header_, const FormatSettings & format_settings_);

    Chunk convertBatch(
        std::unique_ptr<ArrowArray> arrow_array,
        std::unique_ptr<ArrowSchema> arrow_schema);

    static constexpr const char * VALUE_KIND_FIELD = "_VALUE_KIND";

private:
    Block header;
    FormatSettings format_settings;
};

}

#endif

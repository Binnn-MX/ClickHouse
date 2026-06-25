#pragma once
#include <config.h>

#if USE_PAIMON_CPP && (USE_ARROW || USE_ORC || USE_PARQUET)

#include <memory>
#include <vector>

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonPredicateConverter.h>

#include <paimon/fs/file_system.h>
#include <paimon/predicate/predicate.h>
#include <paimon/table/source/plan.h>
#include <paimon/table/source/split.h>

namespace DB
{

class ReadFromPaimonSDKStep : public SourceStepWithFilter
{
public:
    static constexpr auto STEP_NAME = "ReadFromPaimonSDKStorage";

    ReadFromPaimonSDKStep(
        const Block & header_,
        const Names & columns_to_read_,
        const NamesAndTypesList & table_schema_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        size_t num_streams_,
        ContextPtr context_,
        std::shared_ptr<paimon::FileSystem> file_system_,
        const std::string & table_path_,
        const FormatSettings & format_settings_);

    std::string getName() const override { return STEP_NAME; }
    QueryPlanStepPtr clone() const override;

    void applyFilters(ActionDAGNodes added_filter_nodes) override;
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    Block header;
    Names columns_to_read;
    NamesAndTypesList table_schema;
    size_t num_streams;
    std::shared_ptr<paimon::FileSystem> file_system;
    std::string table_path;
    FormatSettings format_settings;
    PaimonPredicateConverter predicate_converter;
    std::shared_ptr<paimon::Predicate> sdk_predicate;
};

}

#endif

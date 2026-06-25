#include <config.h>

#if USE_PAIMON_CPP && (USE_ARROW || USE_ORC || USE_PARQUET)

#include <Storages/ObjectStorage/DataLakes/Paimon/ReadFromPaimonSDKStep.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonSDKSource.h>

#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <paimon/scan_context.h>
#include <paimon/read_context.h>
#include <paimon/table/source/table_scan.h>
#include <paimon/table/source/table_read.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ReadFromPaimonSDKStep::ReadFromPaimonSDKStep(
    const Block & header_,
    const Names & columns_to_read_,
    const NamesAndTypesList & table_schema_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    size_t num_streams_,
    ContextPtr context_,
    std::shared_ptr<paimon::FileSystem> file_system_,
    const std::string & table_path_,
    const FormatSettings & format_settings_)
    : SourceStepWithFilter(
        std::make_shared<const Block>(header_),
        columns_to_read_,
        query_info_,
        storage_snapshot_,
        context_)
    , header(header_)
    , columns_to_read(columns_to_read_)
    , table_schema(table_schema_)
    , num_streams(num_streams_)
    , file_system(std::move(file_system_))
    , table_path(table_path_)
    , format_settings(format_settings_)
    , predicate_converter(table_schema_)
{
}

QueryPlanStepPtr ReadFromPaimonSDKStep::clone() const
{
    return std::make_unique<ReadFromPaimonSDKStep>(*this);
}

void ReadFromPaimonSDKStep::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    if (filter_actions_dag)
        sdk_predicate = predicate_converter.convert(filter_actions_dag.get());
}

void ReadFromPaimonSDKStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto log = getLogger("ReadFromPaimonSDKStep");

    paimon::ScanContextBuilder scan_builder(table_path);
    scan_builder.WithFileSystem(file_system);
    if (sdk_predicate)
        scan_builder.SetPredicate(sdk_predicate);

    auto scan_context_result = scan_builder.Finish();
    if (!scan_context_result.ok())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Failed to build paimon ScanContext: {}",
            scan_context_result.status().ToString());

    auto scan_context = std::move(scan_context_result).value();
    auto table_scan_result = paimon::TableScan::Create(std::move(scan_context));
    if (!table_scan_result.ok())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Failed to create paimon TableScan: {}",
            table_scan_result.status().ToString());

    auto table_scan = std::move(table_scan_result).value();
    auto plan_result = table_scan->CreatePlan();
    if (!plan_result.ok())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Failed to create paimon scan plan: {}",
            plan_result.status().ToString());

    auto plan = std::move(plan_result).value();
    const auto & splits = plan->Splits();

    LOG_DEBUG(log, "Paimon SDK scan produced {} splits", splits.size());

    if (splits.empty())
    {
        auto source_header = std::make_shared<const Block>(header);
        auto pipe = Pipe(std::make_shared<NullSource>(source_header));
        for (const auto & processor : pipe.getProcessors())
            processors.emplace_back(processor);
        pipeline.init(std::move(pipe));
        return;
    }

    paimon::ReadContextBuilder read_builder(table_path);
    read_builder.WithFileSystem(file_system);
    read_builder.SetReadSchema(std::vector<std::string>(columns_to_read.begin(), columns_to_read.end()));
    read_builder.EnablePrefetch(true);
    if (sdk_predicate)
        read_builder.SetPredicate(sdk_predicate);

    auto read_context_result = read_builder.Finish();
    if (!read_context_result.ok())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Failed to build paimon ReadContext: {}",
            read_context_result.status().ToString());

    auto read_context = std::move(read_context_result).value();
    auto table_read_result = paimon::TableRead::Create(std::move(read_context));
    if (!table_read_result.ok())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Failed to create paimon TableRead: {}",
            table_read_result.status().ToString());

    auto table_read = std::move(table_read_result).value();

    auto reader_result = table_read->CreateReader(splits);
    if (!reader_result.ok())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Failed to create paimon BatchReader: {}",
            reader_result.status().ToString());

    auto batch_reader = std::shared_ptr<paimon::BatchReader>(std::move(reader_result).value().release());

    auto shared_mutex = std::make_shared<std::mutex>();

    Pipes pipes;
    auto actual_streams = std::min(num_streams, std::max<size_t>(1, splits.size()));
    for (size_t i = 0; i < actual_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<PaimonSDKSource>(
            batch_reader, shared_mutex, header, format_settings));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    if (pipe.empty())
    {
        auto source_header = std::make_shared<const Block>(header);
        pipe = Pipe(std::make_shared<NullSource>(source_header));
    }

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

}

#endif

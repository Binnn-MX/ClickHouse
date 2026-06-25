#include <config.h>

#if USE_PAIMON_CPP && (USE_ARROW || USE_ORC || USE_PARQUET)

#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonArrowBridge.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <arrow/c/bridge.h>
#include <arrow/compute/api_vector.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

PaimonArrowBridge::PaimonArrowBridge(const Block & header_, const FormatSettings & format_settings_)
    : header(header_)
    , format_settings(format_settings_)
{
}

Chunk PaimonArrowBridge::convertBatch(
    std::unique_ptr<ArrowArray> arrow_array,
    std::unique_ptr<ArrowSchema> arrow_schema)
{
    if (!arrow_array || arrow_array->length == 0)
        return {};

    auto record_batch_result = arrow::ImportRecordBatch(arrow_array.get(), arrow_schema.get());
    if (!record_batch_result.ok())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Failed to import Arrow record batch from paimon-cpp SDK: {}",
            record_batch_result.status().ToString());

    auto record_batch = *record_batch_result;
    auto schema = record_batch->schema();

    int value_kind_idx = schema->GetFieldIndex(VALUE_KIND_FIELD);
    if (value_kind_idx < 0)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "paimon-cpp SDK batch missing {} field", VALUE_KIND_FIELD);

    auto value_kind_array = record_batch->column(value_kind_idx);

    auto row_kind_int8 = std::dynamic_pointer_cast<arrow::Int8Array>(value_kind_array);
    if (!row_kind_int8)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "{} field is not Int8 type", VALUE_KIND_FIELD);

    std::vector<int> columns_to_keep;
    columns_to_keep.reserve(schema->num_fields() - 1);
    for (int i = 0; i < schema->num_fields(); ++i)
    {
        if (i != value_kind_idx)
            columns_to_keep.push_back(i);
    }

    std::vector<int32_t> insert_row_indices;
    insert_row_indices.reserve(record_batch->num_rows());
    for (int64_t row = 0; row < record_batch->num_rows(); ++row)
    {
        auto kind = row_kind_int8->Value(row);
        if (kind == static_cast<int8_t>(PaimonRowKind::INSERT)
            || kind == static_cast<int8_t>(PaimonRowKind::UPDATE_AFTER))
        {
            insert_row_indices.push_back(static_cast<int32_t>(row));
        }
    }

    if (insert_row_indices.empty())
        return {};

    auto filtered_batch = record_batch->SelectColumns(columns_to_keep).ValueOrDie();

    std::shared_ptr<arrow::RecordBatch> final_batch;
    if (static_cast<int64_t>(insert_row_indices.size()) == record_batch->num_rows())
    {
        final_batch = filtered_batch;
    }
    else
    {
        auto indices_builder = arrow::Int32Builder();
        auto status = indices_builder.AppendValues(insert_row_indices.data(), static_cast<int64_t>(insert_row_indices.size()));
        if (!status.ok())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to build filter indices: {}", status.ToString());

        auto indices_result = indices_builder.Finish();
        if (!indices_result.ok())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to finish filter indices: {}", indices_result.status().ToString());

        auto take_result = arrow::compute::Take(filtered_batch, *indices_result);
        if (!take_result.ok())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Arrow Take failed: {}", take_result.status().ToString());

        final_batch = (*take_result).record_batch();
    }

    auto table_result = arrow::Table::FromRecordBatches({final_batch});
    if (!table_result.ok())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to create Arrow table: {}", table_result.status().ToString());

    auto table = *table_result;
    size_t num_rows = table->num_rows();
    if (num_rows == 0)
        return {};

    ArrowColumnToCHColumn converter(
        header,
        "PaimonSDK",
        format_settings,
        /*parquet_columns_to_clickhouse=*/std::nullopt,
        /*clickhouse_columns_to_parquet=*/std::nullopt,
        /*allow_missing_columns=*/format_settings.parquet.allow_missing_columns,
        /*null_as_default=*/format_settings.null_as_default,
        /*date_time_overflow_behavior=*/format_settings.date_time_overflow_behavior,
        /*allow_geoparquet_parser=*/false,
        /*case_insensitive_matching=*/format_settings.parquet.case_insensitive_column_matching,
        /*is_stream=*/false,
        /*enable_json_parsing=*/format_settings.parquet.enable_json_parsing);

    return converter.arrowTableToCHChunk(table, num_rows, /*metadata=*/nullptr, /*block_missing_values=*/nullptr);
}

}

#endif

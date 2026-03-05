#include <config.h>

#if USE_AVRO

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Core/ProtocolDefines.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonTableState.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}
}

namespace DB::Paimon
{

void TableStateSnapshot::serialize(WriteBuffer & out) const
{
    writeIntBinary(snapshot_id, out);
    writeIntBinary(schema_id, out);
    writeStringBinary(base_manifest_list_path, out);
    writeStringBinary(delta_manifest_list_path, out);
    writeStringBinary(commit_kind, out);
    writeIntBinary(commit_time_millis, out);
}

TableStateSnapshot TableStateSnapshot::deserialize(ReadBuffer & in, const int datalake_state_protocol_version)
{
    if (datalake_state_protocol_version <= 0 || datalake_state_protocol_version > DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Cannot deserialize Paimon::TableStateSnapshot with protocol version {}, maximum supported version is {}",
            datalake_state_protocol_version,
            DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION);

    TableStateSnapshot state;
    readIntBinary(state.snapshot_id, in);
    readIntBinary(state.schema_id, in);
    readStringBinary(state.base_manifest_list_path, in);
    readStringBinary(state.delta_manifest_list_path, in);
    readStringBinary(state.commit_kind, in);
    readIntBinary(state.commit_time_millis, in);
    return state;
}

}

#endif

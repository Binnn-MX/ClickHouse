#pragma once
#include <config.h>

#if USE_PAIMON_CPP

#include <memory>
#include <string>
#include <vector>

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/ActionsDAG.h>

#include <paimon/predicate/predicate.h>
#include <paimon/predicate/predicate_builder.h>
#include <paimon/predicate/literal.h>
#include <paimon/defs.h>

namespace DB
{

class PaimonPredicateConverter
{
public:
    explicit PaimonPredicateConverter(const NamesAndTypesList & schema_);

    std::shared_ptr<paimon::Predicate> convert(const ActionsDAG * filter_dag) const;

private:
    std::shared_ptr<paimon::Predicate> convertNode(const ActionsDAG::Node * node) const;

    std::shared_ptr<paimon::Predicate> convertComparison(
        const std::string & func_name,
        const ActionsDAG::Node * column_node,
        const ActionsDAG::Node * const_node) const;

    static paimon::FieldType mapClickHouseTypeToPaimon(const DataTypePtr & type);
    static std::optional<paimon::Literal> fieldToLiteral(const Field & field, paimon::FieldType paimon_type);

    int32_t getFieldIndex(const std::string & name) const;

    NamesAndTypesList schema;
    std::unordered_map<std::string, size_t> name_to_index;
};

}

#endif

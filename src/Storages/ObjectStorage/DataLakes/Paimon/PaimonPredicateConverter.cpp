#include <config.h>

#if USE_PAIMON_CPP

#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonPredicateConverter.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace
{
    LoggerPtr logger() { return getLogger("PaimonPredicateConverter"); }

    bool isComparisonFunction(const std::string & name)
    {
        return name == "equals" || name == "notEquals"
            || name == "less" || name == "greater"
            || name == "lessOrEquals" || name == "greaterOrEquals";
    }

    bool isColumnNode(const ActionsDAG::Node * node)
    {
        return node->type == ActionsDAG::ActionType::INPUT;
    }

    bool isConstantNode(const ActionsDAG::Node * node)
    {
        return node->type == ActionsDAG::ActionType::COLUMN && node->column != nullptr;
    }
}

PaimonPredicateConverter::PaimonPredicateConverter(const NamesAndTypesList & schema_)
    : schema(schema_)
{
    size_t idx = 0;
    for (const auto & col : schema)
    {
        name_to_index[col.name] = idx;
        ++idx;
    }
}

int32_t PaimonPredicateConverter::getFieldIndex(const std::string & name) const
{
    auto it = name_to_index.find(name);
    if (it == name_to_index.end())
        return -1;
    return static_cast<int32_t>(it->second);
}

paimon::FieldType PaimonPredicateConverter::mapClickHouseTypeToPaimon(const DataTypePtr & type)
{
    auto base_type = removeNullable(type);
    WhichDataType which(base_type);

    if (which.isUInt8() || which.isBool())
        return paimon::FieldType::BOOLEAN;
    if (which.isInt8())
        return paimon::FieldType::TINYINT;
    if (which.isInt16())
        return paimon::FieldType::SMALLINT;
    if (which.isInt32())
        return paimon::FieldType::INT;
    if (which.isInt64())
        return paimon::FieldType::BIGINT;
    if (which.isFloat32())
        return paimon::FieldType::FLOAT;
    if (which.isFloat64())
        return paimon::FieldType::DOUBLE;
    if (which.isString() || which.isFixedString())
        return paimon::FieldType::STRING;
    if (which.isDate() || which.isDate32())
        return paimon::FieldType::DATE;
    if (which.isDateTime() || which.isDateTime64())
        return paimon::FieldType::TIMESTAMP;
    if (which.isDecimal32() || which.isDecimal64() || which.isDecimal128() || which.isDecimal256())
        return paimon::FieldType::DECIMAL;

    return paimon::FieldType::UNKNOWN;
}

std::optional<paimon::Literal> PaimonPredicateConverter::fieldToLiteral(const Field & field, paimon::FieldType paimon_type)
{
    if (field.isNull())
        return paimon::Literal(paimon_type);

    switch (paimon_type)
    {
        case paimon::FieldType::BOOLEAN:
            return paimon::Literal(static_cast<bool>(field.safeGet<UInt64>()));
        case paimon::FieldType::TINYINT:
            return paimon::Literal(static_cast<int8_t>(field.safeGet<Int64>()));
        case paimon::FieldType::SMALLINT:
            return paimon::Literal(static_cast<int16_t>(field.safeGet<Int64>()));
        case paimon::FieldType::INT:
            return paimon::Literal(static_cast<int32_t>(field.safeGet<Int64>()));
        case paimon::FieldType::BIGINT:
            return paimon::Literal(field.safeGet<Int64>());
        case paimon::FieldType::FLOAT:
            return paimon::Literal(static_cast<float>(field.safeGet<Float64>()));
        case paimon::FieldType::DOUBLE:
            return paimon::Literal(field.safeGet<Float64>());
        case paimon::FieldType::STRING:
        {
            const auto & str = field.safeGet<String>();
            return paimon::Literal(paimon::FieldType::STRING, str.data(), str.size());
        }
        case paimon::FieldType::DATE:
        {
            auto days = static_cast<int32_t>(field.safeGet<UInt64>());
            return paimon::Literal(paimon::FieldType::DATE, days);
        }
        default:
            return std::nullopt;
    }
}

std::shared_ptr<paimon::Predicate> PaimonPredicateConverter::convert(const ActionsDAG * filter_dag) const
{
    if (!filter_dag)
        return nullptr;

    const auto & outputs = filter_dag->getOutputs();
    if (outputs.empty())
        return nullptr;

    auto result = convertNode(outputs[0]);
    if (!result)
        LOG_DEBUG(logger(), "Could not convert filter DAG to paimon predicate, will skip pushdown");

    return result;
}

std::shared_ptr<paimon::Predicate> PaimonPredicateConverter::convertNode(const ActionsDAG::Node * node) const
{
    if (!node)
        return nullptr;

    if (node->type == ActionsDAG::ActionType::FUNCTION)
    {
        const auto & func_name = node->function_base->getName();
        const auto & children = node->children;

        if (func_name == "and" && children.size() >= 2)
        {
            std::vector<std::shared_ptr<paimon::Predicate>> predicates;
            for (const auto * child : children)
            {
                auto pred = convertNode(child);
                if (pred)
                    predicates.push_back(std::move(pred));
            }
            if (predicates.empty())
                return nullptr;
            if (predicates.size() == 1)
                return predicates[0];
            auto result = paimon::PredicateBuilder::And(predicates);
            return result.ok() ? std::move(result).value() : nullptr;
        }

        if (func_name == "or" && children.size() >= 2)
        {
            std::vector<std::shared_ptr<paimon::Predicate>> predicates;
            for (const auto * child : children)
            {
                auto pred = convertNode(child);
                if (!pred)
                    return nullptr;
                predicates.push_back(std::move(pred));
            }
            if (predicates.empty())
                return nullptr;
            if (predicates.size() == 1)
                return predicates[0];
            auto result = paimon::PredicateBuilder::Or(predicates);
            return result.ok() ? std::move(result).value() : nullptr;
        }

        if (func_name == "not" && children.size() == 1)
        {
            auto pred = convertNode(children[0]);
            if (!pred)
                return nullptr;
            auto result = paimon::PredicateBuilder::Not(pred);
            return result.ok() ? std::move(result).value() : nullptr;
        }

        if (isComparisonFunction(func_name) && children.size() == 2)
        {
            const auto * lhs = children[0];
            const auto * rhs = children[1];

            if (isColumnNode(lhs) && isConstantNode(rhs))
                return convertComparison(func_name, lhs, rhs);
            if (isConstantNode(lhs) && isColumnNode(rhs))
            {
                std::string flipped = func_name;
                if (func_name == "less") flipped = "greater";
                else if (func_name == "greater") flipped = "less";
                else if (func_name == "lessOrEquals") flipped = "greaterOrEquals";
                else if (func_name == "greaterOrEquals") flipped = "lessOrEquals";
                return convertComparison(flipped, rhs, lhs);
            }
            return nullptr;
        }

        if (func_name == "isNull" && children.size() == 1 && isColumnNode(children[0]))
        {
            const auto & col_name = children[0]->result_name;
            int32_t idx = getFieldIndex(col_name);
            if (idx < 0)
                return nullptr;
            auto it = schema.begin();
            std::advance(it, idx);
            auto paimon_type = mapClickHouseTypeToPaimon(it->type);
            if (paimon_type == paimon::FieldType::UNKNOWN)
                return nullptr;
            return paimon::PredicateBuilder::IsNull(idx, col_name, paimon_type);
        }

        if (func_name == "isNotNull" && children.size() == 1 && isColumnNode(children[0]))
        {
            const auto & col_name = children[0]->result_name;
            int32_t idx = getFieldIndex(col_name);
            if (idx < 0)
                return nullptr;
            auto it = schema.begin();
            std::advance(it, idx);
            auto paimon_type = mapClickHouseTypeToPaimon(it->type);
            if (paimon_type == paimon::FieldType::UNKNOWN)
                return nullptr;
            return paimon::PredicateBuilder::IsNotNull(idx, col_name, paimon_type);
        }

        if (func_name == "in" && children.size() == 2 && isColumnNode(children[0]) && isConstantNode(children[1]))
        {
            const auto & col_name = children[0]->result_name;
            int32_t idx = getFieldIndex(col_name);
            if (idx < 0)
                return nullptr;
            auto it = schema.begin();
            std::advance(it, idx);
            auto paimon_type = mapClickHouseTypeToPaimon(it->type);
            if (paimon_type == paimon::FieldType::UNKNOWN)
                return nullptr;

            Field set_field;
            children[1]->column->get(0, set_field);
            if (set_field.getType() != Field::Types::Tuple)
                return nullptr;

            const auto & tuple = set_field.safeGet<Tuple>();
            std::vector<paimon::Literal> literals;
            literals.reserve(tuple.size());
            for (const auto & elem : tuple)
            {
                auto lit = fieldToLiteral(elem, paimon_type);
                if (!lit)
                    return nullptr;
                literals.push_back(std::move(*lit));
            }
            return paimon::PredicateBuilder::In(idx, col_name, paimon_type, literals);
        }
    }

    return nullptr;
}

std::shared_ptr<paimon::Predicate> PaimonPredicateConverter::convertComparison(
    const std::string & func_name,
    const ActionsDAG::Node * column_node,
    const ActionsDAG::Node * const_node) const
{
    const auto & col_name = column_node->result_name;
    int32_t idx = getFieldIndex(col_name);
    if (idx < 0)
        return nullptr;

    auto it = schema.begin();
    std::advance(it, idx);
    auto paimon_type = mapClickHouseTypeToPaimon(it->type);
    if (paimon_type == paimon::FieldType::UNKNOWN)
        return nullptr;

    Field const_value;
    const_node->column->get(0, const_value);

    auto literal = fieldToLiteral(const_value, paimon_type);
    if (!literal)
        return nullptr;

    if (func_name == "equals")
        return paimon::PredicateBuilder::Equal(idx, col_name, paimon_type, *literal);
    if (func_name == "notEquals")
        return paimon::PredicateBuilder::NotEqual(idx, col_name, paimon_type, *literal);
    if (func_name == "less")
        return paimon::PredicateBuilder::LessThan(idx, col_name, paimon_type, *literal);
    if (func_name == "lessOrEquals")
        return paimon::PredicateBuilder::LessOrEqual(idx, col_name, paimon_type, *literal);
    if (func_name == "greater")
        return paimon::PredicateBuilder::GreaterThan(idx, col_name, paimon_type, *literal);
    if (func_name == "greaterOrEquals")
        return paimon::PredicateBuilder::GreaterOrEqual(idx, col_name, paimon_type, *literal);

    return nullptr;
}

}

#endif

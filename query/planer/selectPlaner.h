#pragma once

#include <unordered_map>
#include <string>
#include <cstddef>
#include "../selectQuery.h"
#include "../../types.h"
#include "../../metastore/metastore.h"

struct SchemaColumn {
    size_t index;     
    ValueType type;
};

struct Schema {
    std::unordered_map<std::string, SchemaColumn> columns;

    const SchemaColumn& get(const std::string& name) const {
        auto it = columns.find(name);
        if (it == columns.end())
            throw std::runtime_error("Unknown column: " + name);
        return it->second;
    }
};

void planExpression(ColumnExpression &expr, const Schema &schema);

SELECT_TABLE_ERROR planSelectQuery(SelectQuery &query, const TableInfo &info);

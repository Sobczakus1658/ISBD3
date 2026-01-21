#include "selectPlaner.h"


void planExpression(ColumnExpression &expr, const Schema &schema) {
    switch (expr.type) {

    case ExprType::LITERAL:

        expr.resultType = expr.literal.value.type;
        return;

    case ExprType::COLUMN_REF: {
        const auto& col = schema.get(expr.columnRef.columnName);
        expr.columnRef.index = col.index;
        expr.columnRef.type  = col.type;
        expr.resultType      = col.type;
        return;
    }

    case ExprType::UNARY_OP: {
        if (!expr.unary.operand) throw std::runtime_error("Unary operand missing");
        planExpression(*expr.unary.operand, schema);

        auto operandType = expr.unary.operand->resultType;

        if (expr.unary.op == Operator::NOT) {
            if (operandType != ValueType::BOOL)
                throw std::runtime_error("NOT expects BOOL");
            expr.resultType = ValueType::BOOL;
        }
        else if (expr.unary.op == Operator::MINUS) {
            if (operandType != ValueType::INT64)
                throw std::runtime_error("MINUS expects INT64");
            expr.resultType = ValueType::INT64;
        }
        return;
    }

    case ExprType::BINARY_OP: {
        if (!expr.binary.left || !expr.binary.right) throw std::runtime_error("Binary operand missing");
        planExpression(*expr.binary.left, schema);
        planExpression(*expr.binary.right, schema);

        auto L = expr.binary.left->resultType;
        auto R = expr.binary.right->resultType;

        switch (expr.binary.op) {
        case Operator::ADD:
        case Operator::SUBTRACT:
        case Operator::MULTIPLY:
        case Operator::DIVIDE:
            if (L != ValueType::INT64 || R != ValueType::INT64)
                throw std::runtime_error("Arithmetic expects INT64");
            expr.resultType = ValueType::INT64;
            return;

        case Operator::AND:
        case Operator::OR:
            if (L != ValueType::BOOL || R != ValueType::BOOL)
                throw std::runtime_error("Logical op expects BOOL");
            expr.resultType = ValueType::BOOL;
            return;

        case Operator::EQUAL:
        case Operator::NOT_EQUAL:
        case Operator::LESS_THAN:
        case Operator::LESS_EQUAL:
        case Operator::GREATER_THAN:
        case Operator::GREATER_EQUAL:
            if (L != R)
                throw std::runtime_error("Comparison requires same types");
            expr.resultType = ValueType::BOOL;
            return;
        }
    }

    case ExprType::FUNCTION: {
        for (auto &arg : expr.function.args) {
            if (!arg) throw std::runtime_error("Function argument missing");
            planExpression(*arg, schema);
        }

        switch (expr.function.name) {
        case FunctionName::STRLEN:
            if (expr.function.args.size() != 1 ||
                expr.function.args[0]->resultType != ValueType::VARCHAR)
                throw std::runtime_error("STRLEN expects VARCHAR");
            expr.resultType = ValueType::INT64;
            return;

        case FunctionName::CONCAT:
            if (expr.function.args.size() != 2 ||
                expr.function.args[0]->resultType != ValueType::VARCHAR ||
                expr.function.args[1]->resultType != ValueType::VARCHAR)
                throw std::runtime_error("CONCAT expects VARCHAR, VARCHAR");
            expr.resultType = ValueType::VARCHAR;
            return;

        case FunctionName::REPLACE:
            if (expr.function.args.size() != 3 ||
                expr.function.args[0]->resultType != ValueType::VARCHAR ||
                expr.function.args[1]->resultType != ValueType::VARCHAR ||
                expr.function.args[2]->resultType != ValueType::VARCHAR)
                throw std::runtime_error("REPLACE expects VARCHAR, VARCHAR, VARCHAR");
            expr.resultType = ValueType::VARCHAR;
            return;

        case FunctionName::UPPER:
        case FunctionName::LOWER:
            if (expr.function.args.size() != 1 ||
                expr.function.args[0]->resultType != ValueType::VARCHAR)
                throw std::runtime_error("UPPER/LOWER expects VARCHAR");
            expr.resultType = ValueType::VARCHAR;
            return;
        }
    }
    }
}


Schema buildSchema(const TableInfo &table) {
    Schema schema;
    size_t idx = 0;

    for (const auto &col : table.info) {
        const std::string &name = col.first;
        const std::string &typeStr = col.second;

        ValueType type;
        if (typeStr == "INT64") {
            type = ValueType::INT64;
        } else if (typeStr == "VARCHAR") {
            type = ValueType::VARCHAR;
        } else if (typeStr == "BOOL") {
            type = ValueType::BOOL;
        } else {
            throw std::runtime_error("Unknown column type: " + typeStr);
        }

        schema.columns.emplace(name, SchemaColumn{ idx++, type });
    }

    return schema;
}


SELECT_TABLE_ERROR planSelectQuery(
    SelectQuery &query,
    const TableInfo &info
) {
    auto schema = buildSchema(info);

    try {
        for (auto &expr : query.columnClauses) {
            planExpression(*expr, schema);
        }

    for (auto &obe : query.orderByClauses) {
            if (!obe.columnName.empty()) {
                bool found = false;
                for (size_t i = 0; i < query.columnClauses.size(); ++i) {
                    const auto &c = query.columnClauses[i];
                    if (!c) continue;
                    if (c->type == ExprType::COLUMN_REF &&
                        c->columnRef.columnName == obe.columnName &&
                        (obe.tableName.empty() || c->columnRef.tableName == obe.tableName)) {
                        obe.columnIndex = i;
                        found = true;
                        break;
                    }
                }
                if (!found) return SELECT_TABLE_ERROR::INVALID_ORDER_BY;
            } else {
                if (obe.columnIndex >= query.columnClauses.size()) return SELECT_TABLE_ERROR::INVALID_ORDER_BY;
            }
        }

        size_t baseCols = schema.columns.size();
        for (auto &obe : query.orderByClauses) {
            obe.columnIndex = baseCols + obe.columnIndex;
        }

        if (query.whereClause) {
            planExpression(*query.whereClause, schema);
            if (query.whereClause->resultType != ValueType::BOOL)
                return SELECT_TABLE_ERROR::INVALID_WHERE;
        }
    } catch (const std::exception &e) {
        return SELECT_TABLE_ERROR::INVALID_WHERE;
    }

    return SELECT_TABLE_ERROR::NONE;
}

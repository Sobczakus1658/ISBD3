#pragma once

#include <memory>
#include <vector>
#include <string>
#include <optional>


struct ColumnExpression;

using ColumnExprPtr = std::unique_ptr<ColumnExpression>;


enum class FunctionName {
    STRLEN,
    CONCAT,
    UPPER,
    LOWER
};

enum class ValueType {
    INT64,
    VARCHAR,
    BOOL
};
enum class ExprType {
    COLUMN_REF,
    LITERAL,
    FUNCTION,
    BINARY_OP,
    UNARY_OP
};

enum class Operator {
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,

    AND,
    OR,

    EQUAL,
    NOT_EQUAL,
    LESS_THAN,
    LESS_EQUAL,
    GREATER_THAN,
    GREATER_EQUAL,

    NOT,
    MINUS
};


struct Value {
    ValueType type;

    int64_t intValue;
    std::string stringValue;
    bool boolValue;
};

struct ColumnReference {
    std::string tableName;
    std::string columnName;
    size_t index;
    ValueType type;
};

struct Literal {
    Value value;
};

struct FunctionExpr {
    FunctionName name;
    std::vector<ColumnExprPtr> args;
};

struct BinaryExpr {
    Operator op;
    ColumnExprPtr left;
    ColumnExprPtr right;
};

struct UnaryExpr {
    Operator op;
    ColumnExprPtr operand;
};

struct ColumnExpression {
    ExprType type;
    ValueType resultType; 
    ColumnReference columnRef;
    Literal literal;
    FunctionExpr function;
    BinaryExpr binary;
    UnaryExpr unary;
};

struct OrderByExpression {
    size_t columnIndex = 0;
    bool ascending = true;
    std::string tableName;
    std::string columnName;
};

struct ResultRow {
    std::vector<Value> values;
};

struct SelectQuery {
    std::string tableName;
    std::vector<std::unique_ptr<ColumnExpression>> columnClauses;
    std::unique_ptr<ColumnExpression> whereClause;
    std::vector<OrderByExpression> orderByClauses;
    std::optional<size_t> limit;
};

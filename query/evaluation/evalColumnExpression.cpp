#include "evalColumnExpression.h"
#include <iostream>

template<typename Op>
Value compare(const Value &a, const Value &b, Op op) {
    switch (a.type) {
    case ValueType::INT64:
        return {ValueType::BOOL, 0, "", op(a.intValue, b.intValue)};
    case ValueType::VARCHAR:
        return {ValueType::BOOL, 0, "", op(a.stringValue, b.stringValue)};
    case ValueType::BOOL:
        return {ValueType::BOOL, 0, "", op(a.boolValue, b.boolValue)};
    }
}

Value evalUnary(const UnaryExpr &u, const ResultRow &row) {
    Value v = evalColumnExpression(*u.operand, row);

    if (u.op == Operator::NOT) {
        return Value{ValueType::BOOL, 0, "", !v.boolValue};
    }

    return Value{ValueType::INT64, -v.intValue, "", false};

}

Value evalBinary(const BinaryExpr &b, const ResultRow &row) {
    Value l = evalColumnExpression(*b.left, row);
    Value r = evalColumnExpression(*b.right, row);

    switch (b.op) {

    case Operator::ADD:
        return {ValueType::INT64, l.intValue + r.intValue};

    case Operator::SUBTRACT:
        return {ValueType::INT64, l.intValue - r.intValue};

    case Operator::MULTIPLY:
        return {ValueType::INT64, l.intValue * r.intValue};

    case Operator::DIVIDE:
        return {ValueType::INT64, l.intValue / r.intValue};

    case Operator::AND:
        return {ValueType::BOOL, 0, "", l.boolValue && r.boolValue};

    case Operator::OR:
        return {ValueType::BOOL, 0, "", l.boolValue || r.boolValue};

    case Operator::EQUAL:
        return compare(l, r, std::equal_to<>());

    case Operator::NOT_EQUAL:
        return compare(l, r, std::not_equal_to<>());

    case Operator::LESS_THAN:
        return compare(l, r, std::less<>());

    case Operator::LESS_EQUAL:
        return compare(l, r, std::less_equal<>());

    case Operator::GREATER_THAN:
        return compare(l, r, std::greater<>());

    case Operator::GREATER_EQUAL:
        return compare(l, r, std::greater_equal<>());
    }
}

Value evalFunction(const FunctionExpr &f, const ResultRow &row) {
    if (f.name == FunctionName::STRLEN) {
        auto v = evalColumnExpression(*f.args[0], row);
        return {ValueType::INT64, (int64_t)v.stringValue.size()};
    }

    if (f.name == FunctionName::CONCAT) {
        if (f.args.size() < 2 || !f.args[0] || !f.args[1]) throw std::runtime_error("CONCAT missing args");
        auto a = evalColumnExpression(*f.args[0], row);
        auto b = evalColumnExpression(*f.args[1], row);
        return {ValueType::VARCHAR, 0, a.stringValue + b.stringValue};
    }

    if (f.name == FunctionName::UPPER) {
        if (f.args.empty() || !f.args[0]) throw std::runtime_error("UPPER missing arg");
        auto v = evalColumnExpression(*f.args[0], row);
        std::string s = v.stringValue;
        for (auto &c : s) c = std::toupper(c);
        return {ValueType::VARCHAR, 0, s};
    }

    if (f.name == FunctionName::LOWER) {
        if (f.args.empty() || !f.args[0]) throw std::runtime_error("LOWER missing arg");
        auto v = evalColumnExpression(*f.args[0], row);
        std::string s = v.stringValue;
        for (auto &c : s) c = std::tolower(c);
        return {ValueType::VARCHAR, 0, s};
    }
}

Value evalColumnExpression(const ColumnExpression &expr, const ResultRow &row) {
    switch (expr.type) {
        case ExprType::LITERAL:
            return expr.literal.value;

        case ExprType::COLUMN_REF:
            if (expr.columnRef.index >= row.values.size())
                throw std::runtime_error("Column index out of range in evaluation");
            return row.values[expr.columnRef.index];

        case ExprType::UNARY_OP:
            return evalUnary(expr.unary, row);

        case ExprType::BINARY_OP:
            return evalBinary(expr.binary, row);

        case ExprType::FUNCTION:
            return evalFunction(expr.function, row);
    }
}
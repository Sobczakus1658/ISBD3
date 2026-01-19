#include "evalColumnExpression.h"
#include "expression_hasher.h"
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
    return Value{ValueType::INT64, 0, std::string(), false};
}

// Internal evaluator that assumes caller manages caching for this node.
static Value evalColumnExpressionInternal(const ColumnExpression &expr, const ResultRow &row, ExpressionCache *cache) {
    switch (expr.type) {
        case ExprType::LITERAL:
            return expr.literal.value;

        case ExprType::COLUMN_REF:
            if (expr.columnRef.index >= row.values.size())
                throw std::runtime_error("Column index out of range in evaluation");
            return row.values[expr.columnRef.index];

        case ExprType::UNARY_OP: {
            Value v = evalColumnExpression(*expr.unary.operand, row, cache);
            if (expr.unary.op == Operator::NOT) {
                return Value{ValueType::BOOL, 0, "", !v.boolValue};
            }
            return Value{ValueType::INT64, -v.intValue, "", false};
        }

        case ExprType::BINARY_OP: {
            Value l = evalColumnExpression(*expr.binary.left, row, cache);
            Value r = evalColumnExpression(*expr.binary.right, row, cache);

            switch (expr.binary.op) {
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
            return Value{ValueType::INT64, 0, std::string(), false};
        }

        case ExprType::FUNCTION: {
            const FunctionExpr &f = expr.function;
            if (f.name == FunctionName::STRLEN) {
                auto v = evalColumnExpression(*f.args[0], row, cache);
                return {ValueType::INT64, (int64_t)v.stringValue.size()};
            }
            if (f.name == FunctionName::CONCAT) {
                if (f.args.size() < 2 || !f.args[0] || !f.args[1]) throw std::runtime_error("CONCAT missing args");
                auto a = evalColumnExpression(*f.args[0], row, cache);
                auto b = evalColumnExpression(*f.args[1], row, cache);
                return {ValueType::VARCHAR, 0, a.stringValue + b.stringValue};
            }
            if (f.name == FunctionName::UPPER) {
                if (f.args.empty() || !f.args[0]) throw std::runtime_error("UPPER missing arg");
                auto v = evalColumnExpression(*f.args[0], row, cache);
                std::string s = v.stringValue;
                for (auto &c : s) c = std::toupper(c);
                return {ValueType::VARCHAR, 0, s};
            }
            if (f.name == FunctionName::LOWER) {
                if (f.args.empty() || !f.args[0]) throw std::runtime_error("LOWER missing arg");
                auto v = evalColumnExpression(*f.args[0], row, cache);
                std::string s = v.stringValue;
                for (auto &c : s) c = std::tolower(c);
                return {ValueType::VARCHAR, 0, s};
            }
            if (f.name == FunctionName::REPLACE) {
                // expects: REPLACE(source, search, replace)
                if (f.args.size() != 3 || !f.args[0] || !f.args[1] || !f.args[2])
                    throw std::runtime_error("REPLACE missing args");
                auto src = evalColumnExpression(*f.args[0], row, cache);
                auto search = evalColumnExpression(*f.args[1], row, cache);
                auto repl = evalColumnExpression(*f.args[2], row, cache);

                std::string s = src.stringValue;
                const std::string &pat = search.stringValue;
                const std::string &rep = repl.stringValue;

                if (!pat.empty()) {
                    size_t pos = 0;
                    while ((pos = s.find(pat, pos)) != std::string::npos) {
                        s.replace(pos, pat.size(), rep);
                        pos += rep.size();
                    }
                }
                // If pat is empty, behavior: return original string (no-op)
                return {ValueType::VARCHAR, 0, s};
            }
            return Value{ValueType::INT64, 0, std::string(), false};
        }
    }
    return Value{ValueType::INT64, 0, std::string(), false};
}

Value evalColumnExpression(const ColumnExpression &expr, const ResultRow &row, ExpressionCache *cache) {
    // If caching is provided, use it for this node. Cache keyed by hashExpression.
    if (cache) {
        size_t h = hashExpression(expr);
        // Use getOrCompute so ExpressionCache logs hits/misses consistently.
        return cache->getOrCompute(h, [&]{ return evalColumnExpressionInternal(expr, row, cache); });
    }

    // No cache provided: compute directly
    return evalColumnExpressionInternal(expr, row, nullptr);
}
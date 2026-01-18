#include "expression_hasher.h"
#include <functional>
#include <string>

using std::size_t;

static inline void mixHash(size_t &h, size_t v) {
    // simple mix: xor + mul by prime
    h ^= v + 0x9e3779b97f4a7c15ULL + (h<<6) + (h>>2);
}

static size_t hashString(const std::string &s) {
    return std::hash<std::string>{}(s);
}

static size_t hashValue(const Value &v) {
    size_t h = 0;
    mixHash(h, (size_t)v.type);
    switch (v.type) {
        case ValueType::INT64: mixHash(h, std::hash<long long>{}(v.intValue)); break;
        case ValueType::VARCHAR: mixHash(h, hashString(v.stringValue)); break;
        case ValueType::BOOL: mixHash(h, std::hash<bool>{}(v.boolValue)); break;
    }
    return h;
}

size_t hashExpression(const ColumnExpression &expr) {
    size_t h = 0;
    mixHash(h, (size_t)expr.type);

    switch (expr.type) {
        case ExprType::LITERAL:
            mixHash(h, hashValue(expr.literal.value));
            break;

        case ExprType::COLUMN_REF:
            // use resolved index (if set) and column name as fallback
            mixHash(h, std::hash<std::string>{}(expr.columnRef.columnName));
            mixHash(h, std::hash<size_t>{}(expr.columnRef.index));
            break;

        case ExprType::UNARY_OP: {
            mixHash(h, (size_t)expr.unary.op);
            if (expr.unary.operand) mixHash(h, hashExpression(*expr.unary.operand));
            break;
        }

        case ExprType::BINARY_OP: {
            mixHash(h, (size_t)expr.binary.op);
            if (!expr.binary.left || !expr.binary.right) break;
            size_t hl = hashExpression(*expr.binary.left);
            size_t hr = hashExpression(*expr.binary.right);

            // commutative operators: order-independent combination
            bool comm = (expr.binary.op == Operator::ADD || expr.binary.op == Operator::MULTIPLY ||
                         expr.binary.op == Operator::AND || expr.binary.op == Operator::OR ||
                         expr.binary.op == Operator::EQUAL || expr.binary.op == Operator::NOT_EQUAL);

            if (comm) {
                if (hl < hr) { mixHash(h, hl); mixHash(h, hr); }
                else { mixHash(h, hr); mixHash(h, hl); }
            } else {
                mixHash(h, hl); mixHash(h, hr);
            }
            break;
        }

        case ExprType::FUNCTION: {
            mixHash(h, (size_t)expr.function.name);
            // functions are order-sensitive
            for (const auto &arg : expr.function.args) {
                if (arg) mixHash(h, hashExpression(*arg));
            }
            break;
        }
    }

    return h;
}

#include "selectQueryParser.h"

Operator parseOperator(const json& j) {
    std::string op = j.get<std::string>();

    if (op == "ADD") return Operator::ADD;
    if (op == "SUBTRACT") return Operator::SUBTRACT;
    if (op == "MULTIPLY") return Operator::MULTIPLY;
    if (op == "DIVIDE") return Operator::DIVIDE;

    if (op == "AND") return Operator::AND;
    if (op == "OR") return Operator::OR;

    if (op == "EQUAL") return Operator::EQUAL;
    if (op == "NOT_EQUAL") return Operator::NOT_EQUAL;
    if (op == "LESS_THAN") return Operator::LESS_THAN;
    if (op == "LESS_EQUAL") return Operator::LESS_EQUAL;
    if (op == "GREATER_THAN") return Operator::GREATER_THAN;
    if (op == "GREATER_EQUAL") return Operator::GREATER_EQUAL;

    if (op == "NOT") return Operator::NOT;
    if (op == "MINUS") return Operator::MINUS;

    throw std::runtime_error("Unknown operator: " + op);
}

FunctionName parseFunction(const json& j) {
    std::string fn = j.get<std::string>();

    if (fn == "STRLEN") return FunctionName::STRLEN;
    if (fn == "CONCAT") return FunctionName::CONCAT;
    if (fn == "UPPER") return FunctionName::UPPER;
    if (fn == "LOWER") return FunctionName::LOWER;

    throw std::runtime_error("Unknown function: " + fn);
}

Value parseValue(const json& j) {
    Value v;

    if (j.is_number_integer()) {
        v.type = ValueType::INT64;
        v.intValue = j.get<int64_t>();
        return v;
    }

    if (j.is_string()) {
        v.type = ValueType::VARCHAR;
        v.stringValue = j.get<std::string>();
        return v;
    }

    if (j.is_boolean()) {
        v.type = ValueType::BOOL;
        v.boolValue = j.get<bool>();
        return v;
    }

    throw std::runtime_error("Invalid literal value");
}

Operator parseUnaryOperator(const json& j) {
    std::string op = j.get<std::string>();

    if (op == "NOT") return Operator::NOT;
    if (op == "MINUS") return Operator::MINUS;

    throw std::runtime_error("Unknown unary operator: " + op);
}

std::unique_ptr<ColumnExpression> parseColumnExpression(const json& j) {
    auto expr = std::make_unique<ColumnExpression>();

    if (j.contains("value")) {
        expr->type = ExprType::LITERAL;
        expr->literal.value = parseValue(j["value"]);
        return expr;
    }

    if (j.contains("columnName")) {
        expr->type = ExprType::COLUMN_REF;
        expr->columnRef.tableName = j["tableName"];
        expr->columnRef.columnName = j["columnName"];
        return expr;
    }

    if (j.contains("functionName")) {
        expr->type = ExprType::FUNCTION;
        expr->function.name = parseFunction(j["functionName"]);

        for (auto& a : j["arguments"]) {
            expr->function.args.push_back(parseColumnExpression(a));
        }
        return expr;
    }

    if (j.contains("leftOperand")) {
        expr->type = ExprType::BINARY_OP;
        expr->binary.op = parseOperator(j["operator"]);
        expr->binary.left = parseColumnExpression(j["leftOperand"]);
        expr->binary.right = parseColumnExpression(j["rightOperand"]);
        return expr;
    }

    if (j.contains("operand")) {
        expr->type = ExprType::UNARY_OP;
        expr->unary.op = parseUnaryOperator(j["operator"]);
        expr->unary.operand = parseColumnExpression(j["operand"]);
        return expr;
    }

    throw std::runtime_error("Invalid ColumnExpression");
}

SelectQuery parse(const json& def) {
    SelectQuery sq;

    
    for (const auto& col : def.at("columnClauses")) {
        sq.columnClauses.push_back(parseColumnExpression(col));
    }

    auto findTableInExpr = [](const ColumnExpression *expr, auto &self) -> std::string {
        if (!expr) return std::string();
        switch (expr->type) {
            case ExprType::COLUMN_REF:
                return expr->columnRef.tableName;
            case ExprType::FUNCTION:
                for (const auto &a : expr->function.args) {
                    if (!a) continue;
                    auto t = self(a.get(), self);
                    if (!t.empty()) return t;
                }
                return std::string();
            case ExprType::BINARY_OP:
                if (expr->binary.left) {
                    auto t = self(expr->binary.left.get(), self);
                    if (!t.empty()) return t;
                }
                if (expr->binary.right) return self(expr->binary.right.get(), self);
                return std::string();
            case ExprType::UNARY_OP:
                if (expr->unary.operand) return self(expr->unary.operand.get(), self);
                return std::string();
            default:
                return std::string();
        }
    };

    for (const auto &c : sq.columnClauses) {
        if (!c) continue;
        auto t = findTableInExpr(c.get(), findTableInExpr);
        if (!t.empty()) { sq.tableName = t; break; }
    }

    if (def.contains("whereClause")) {
        sq.whereClause = parseColumnExpression(def["whereClause"]);
    }

    if (sq.tableName.empty() && sq.whereClause) {
        auto t = findTableInExpr(sq.whereClause.get(), findTableInExpr);
        if (!t.empty()) sq.tableName = t;
    }

    if (def.contains("limitClause")) {
        sq.limit = def.at("limitClause").at("limit").get<size_t>();
    }

    if (def.contains("orderByClauses")) {
        for (const auto &o : def.at("orderByClauses")) {
            OrderByExpression obe;
            if (o.contains("tableName")) obe.tableName = o.value("tableName", std::string());
            if (o.contains("columnName")) obe.columnName = o.value("columnName", std::string());
            std::string dir = o.value("direction", std::string("ASC"));
            obe.ascending = (dir == "ASC");
            sq.orderByClauses.push_back(std::move(obe));
        }
    }


    return sq;
}

#include "errors.h"
#include <fstream>
#include <iostream>

namespace fs = std::filesystem;
static const fs::path basePath =  fs::current_path() / "errors/errors.json";

json readFileErrors(){
    json result;
    ifstream err(basePath);
    if (!err.is_open()) {
        return json::array();
    }
    err >> result;
    return result;
}

std::optional<QueryError> getQueryError(std::string id){
    json data = readFileErrors();
    for (const auto &entry : data) {
        if (!entry.is_object()) continue;
        std::string entryId = entry.value("id", std::string());
        if (entryId != id) continue;

        QueryError qe;
        if (entry.contains("problems") && entry["problems"].is_array()) {
            for (const auto &p : entry["problems"]) {
                if (!p.is_object()) continue;
                Problem prob;
                prob.error = p.value("error", std::string());
                if (p.contains("context") && p["context"].is_string()) prob.context = p["context"].get<std::string>();
                qe.problems.push_back(std::move(prob));
            }
        }
        return qe;
    }
    return std::nullopt;
}

void addError(std::string id, json multipleProblemsError){
    json data = readFileErrors();

    json problems = json::array();
    if (multipleProblemsError.is_object() && multipleProblemsError.contains("problems") && multipleProblemsError["problems"].is_array()) {
        problems = multipleProblemsError["problems"];
    } else if (multipleProblemsError.is_array()) {
        problems = multipleProblemsError;
    }

    json entry = json::object();
    entry["id"] = id;
    entry["problems"] = json::array();
    for (const auto &p : problems) entry["problems"].push_back(p);
    data.push_back(std::move(entry));

    std::ofstream out(basePath);
    out << std::setw(2) << data << std::endl;
    out.close();
}
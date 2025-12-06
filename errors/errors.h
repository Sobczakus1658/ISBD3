#include "../types.h"
#include <string>
#include <nlohmann/json.hpp>
#include <vector>
#include <optional>


using json = nlohmann::ordered_json;

std::optional<QueryError> getQueryError(std::string id);

void addError(std::string id, json multipleProblemsError);
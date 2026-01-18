#include "../selectQuery.h"
#include <nlohmann/json.hpp>

using json = nlohmann::ordered_json;

SelectQuery parseSelect(const json& def);

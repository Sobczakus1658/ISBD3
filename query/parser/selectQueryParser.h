#include "../selectQuery.h"
#include <nlohmann/json.hpp>

using json = nlohmann::ordered_json;

SelectQuery parse(const json& def);

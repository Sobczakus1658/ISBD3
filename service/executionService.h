#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <map>

#include "../metastore/metastore.h"
#include "../serialization/serializator.h"
#include "../types.h"

QueryCreatedResponse copyCSV(CopyQuery  query);

std::string selectTable(std::string name);


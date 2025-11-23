#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <map>

#include "../metastore/metastore.h"
#include "../serialization/serializator.h"
#include "../types.h"

QueryCreatedResponse copyCSV(CopyQuery  query, string query_id);

SELECT_TABLE_ERROR selectTable(SelectQuery select_query, string query_id);


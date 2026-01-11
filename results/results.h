#include "../types.h"
#include <optional>

std::optional<QueryResult> getQueryResult(const std::string &id, int rowLimit = 0);

void modifyResult(std::string id, std::vector<MixBatch>& batches);

void initResult(std::string id);

void removeResult(const std::string &id);

void finalizeResult(const std::string &id, const std::vector<OrderByExpression> &orderBy, const std::optional<size_t> &limit);
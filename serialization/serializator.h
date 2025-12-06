#include "../types.h"
#include <string>
#include <vector>


std::vector<std::string> serializator(std::vector<Batch> &batches, const std::string& filepath, uint64_t PART_LIMIT);
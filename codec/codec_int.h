#include <vector>
#include <string>
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <utility>

#include "../types.h"


uint64_t encodeSingleIntColumn(std::ofstream& out, IntColumn& column);

void decodeIntColumns(std::ifstream& in, std::vector<IntColumn>& columns, uint32_t length); 

std::pair<uint64_t, IntColumn> decodeIntColumn(std::ifstream& in);

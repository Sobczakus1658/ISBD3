#include <vector>
#include <string>
#include <iostream>
#include <fstream>
#include <utility>

#include "../types.h"


uint64_t encodeSingleStringColumn(std::ofstream& out, StringColumn& column);

void decodeStringColumns(std::ifstream& in, std::vector<StringColumn>& columns, uint32_t length); 

std::pair<uint64_t, StringColumn> decodeStringColumn(std::ifstream& in);

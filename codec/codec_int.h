#include <vector>
#include <string>
#include <iostream>
#include <fstream>
#include <unordered_map>

#include "../types.h"


uint64_t encodeSingleIntColumn(ofstream& out, IntColumn& column);

void decodeIntColumns(ifstream& in, vector<IntColumn>& columns, uint32_t length); 

pair<uint64_t, IntColumn> decodeIntColumn(ifstream& in);

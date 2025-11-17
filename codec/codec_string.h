#include <vector>
#include <string>
#include <iostream>
#include <fstream>

#include "../types.h"


uint64_t encodeSingleStringColumn(ofstream& out, StringColumn& column);

void decodeStringColumns(ifstream& in, vector<StringColumn>& columns, uint32_t length); 

pair<uint64_t, StringColumn> decodeStringColumn(ifstream& in);

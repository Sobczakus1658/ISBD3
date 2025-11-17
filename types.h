#include <vector>
#pragma once

#include <vector>
#include <string>
#include <cstdint>
#include <cstddef>

inline constexpr size_t BATCH_SIZE = 8192;
inline constexpr int compresion_level = 3;
inline constexpr uint32_t file_magic = 0x21374201;
inline constexpr uint32_t batch_magic = 0x69696969;
static constexpr uint8_t INTEGER = 0;
static constexpr uint8_t STRING  = 1;

using namespace std;

using ColumnInfo = pair<uint64_t, uint8_t>;

struct IntColumn {
    string name;
    vector<int64_t> column;
};

struct StringColumn {
    string name;
    vector<string> column;
};

struct Batch {
    vector<IntColumn> intColumns;
    vector<StringColumn> stringColumns;
    size_t num_rows;
};


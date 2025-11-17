#include "codec_int.h"
#include <algorithm>
#include <iostream>

struct EncodeIntColumn {
    string name;
    vector<uint8_t> compressed_data;
    int64_t delta_base;
};

static void variableLengthEncoding(vector<uint8_t> &compressed_data, const vector<uint64_t> &column) {
    for (uint64_t v : column) {
        uint64_t value = v;
        while (true) {
            uint8_t byte = static_cast<uint8_t>(value & 0x7F);
            value >>= 7;
            if (value == 0) {
                compressed_data.push_back(byte);
                break;
            } else {
                compressed_data.push_back(byte | 0x80);
            }
        }
    }
}

static void variableLengthDecoding(const vector<uint8_t> &compressed_data, vector<uint64_t> &out) {
    uint64_t value = 0;
    unsigned shift = 0;
    for (uint8_t chunk : compressed_data) {
        value |= (uint64_t)(chunk & 0x7F) << shift;
        if ((chunk & 0x80) == 0) {
            out.push_back(value);
            value = 0;
            shift = 0;
        } else {
            shift += 7;
        }
    }
}

static vector<uint64_t> deltaEncoding(const vector<int64_t> &column, int64_t base) {
    vector<uint64_t> out;
    out.reserve(column.size());
    for (size_t i = 0; i < column.size(); ++i) {
        int64_t diff = column[i] - base;
        out.push_back(static_cast<uint64_t>(diff));
    }
    return out;
}

static vector<int64_t> deltaDecoding(const vector<uint64_t> &diffs, int64_t base) {
    vector<int64_t> out;
    out.reserve(diffs.size());
    for (size_t i = 0; i < diffs.size(); ++i) {
        out.push_back(static_cast<int64_t>(diffs[i]) + base);
    }
    return out;
}

EncodeIntColumn compressIntColumn(IntColumn& column) {
    EncodeIntColumn out;
    out.name = column.name;
    if (column.column.empty()) {
        out.delta_base = 0;
        out.compressed_data.clear();
        return out;
    }
    auto min_it = min_element(column.column.begin(), column.column.end());
    out.delta_base = *min_it;
    vector<uint64_t> modified = deltaEncoding(column.column, out.delta_base);
    variableLengthEncoding(out.compressed_data, modified);
    return out;
}

IntColumn decodeSingleIntColumn(EncodeIntColumn& column) {
    IntColumn out;
    out.name = column.name;
    vector<uint64_t> diffs;
    variableLengthDecoding(column.compressed_data, diffs);
    out.column = deltaDecoding(diffs, column.delta_base);
    return out;
}

pair<uint64_t, IntColumn> decodeIntColumn(ifstream &in) {
    EncodeIntColumn column;
    uint64_t prev_ptr = 0;
    in.read((char *)(&prev_ptr), sizeof(prev_ptr));

    uint32_t name_len = 0;
    in.read((char *)(&name_len), sizeof(name_len));
    column.name.resize(name_len);
    if (name_len > 0) {
        in.read(&column.name[0], name_len);
    }

    in.read((char *)(&column.delta_base), sizeof(column.delta_base));

    uint32_t compressed_bits_length = 0;
    in.read((char *)(&compressed_bits_length), sizeof(compressed_bits_length));
    column.compressed_data.resize(static_cast<size_t>(compressed_bits_length));
    if (compressed_bits_length > 0) {
        in.read((char *)(column.compressed_data.data()), compressed_bits_length);
    }

    IntColumn out = decodeSingleIntColumn(column);
    return {prev_ptr, move(out)};
}

uint64_t encodeSingleIntColumn(ofstream& out, IntColumn& column){
    EncodeIntColumn col = compressIntColumn(column);
    uint32_t len = static_cast<uint32_t>(col.name.size());
    uint32_t compressed_bits_length = static_cast<uint32_t>(col.compressed_data.size());
    int64_t delta_base = col.delta_base;

    out.write((char*)&len, sizeof(len));
    out.write(col.name.data(), len);

    out.write((char*)&delta_base, sizeof(delta_base));

    out.write((char*)&compressed_bits_length, sizeof(compressed_bits_length));
    if (compressed_bits_length > 0) {
        out.write((char*)(col.compressed_data.data()), compressed_bits_length);
    }

    uint64_t total = 0;
    total += sizeof(len);
    total += static_cast<uint64_t>(len);
    total += sizeof(delta_base);
    total += sizeof(compressed_bits_length);
    total += static_cast<uint64_t>(compressed_bits_length);
    return total;
}

void decodeIntColumns(ifstream& in, vector<IntColumn>& columns, uint32_t length) {
    for (uint32_t j = 0; j < length; j++) {
        columns.push_back(decodeIntColumn(in).second);
    }
}
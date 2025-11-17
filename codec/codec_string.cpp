#include "codec_string.h"
#include <zstd.h>

struct EncodeStringColumn {
    string name;
    vector<uint8_t> compressed_data;
    uint32_t uncompressed_size;  
    uint32_t compressed_size;
};

StringColumn decodeSingleStringColumn(EncodeStringColumn& column) {

    StringColumn out;
    out.name = move(column.name);

    string decompressed(column.uncompressed_size, '\0');
    ZSTD_decompress(
        decompressed.data(),
        column.uncompressed_size,
        column.compressed_data.data(),
        column.compressed_size
    );

    size_t start = 0;
    for (size_t i = 0; i < decompressed.size(); ++i) {
        if (decompressed[i] == '\0') {
            out.column.emplace_back(decompressed.substr(start, i - start));
            start = i + 1;
        }
    }
    return out;
}

pair<uint64_t, StringColumn> decodeStringColumn(ifstream& in) {
    EncodeStringColumn column;
    uint64_t prev_ptr = 0;
    in.read((char *)(&prev_ptr), sizeof(prev_ptr));

    string name;
    uint32_t name_len;
    uint32_t uncompressed_size;
    uint32_t compressed_size;

    in.read((char *)(&name_len), sizeof(name_len));
    name.resize(name_len);
    if (name_len > 0) {
        in.read(&name[0], name_len);
    }
    column.name = move(name);

    in.read((char *)(&uncompressed_size), sizeof(uncompressed_size));
    in.read((char *)(&compressed_size), sizeof(compressed_size));
    column.uncompressed_size = uncompressed_size;
    column.compressed_size = compressed_size;
    column.compressed_data.resize(static_cast<size_t>(compressed_size));
    if (compressed_size > 0) {
        in.read((char *)(column.compressed_data.data()), compressed_size);
    }

    return {prev_ptr, decodeSingleStringColumn(column)};
}

EncodeStringColumn* compressStringColumn(StringColumn& column) {
    auto* out = new EncodeStringColumn();

    size_t total_size = 0;
    for (const auto &s : column.column) {
        total_size += s.size() + 1; 
    }

    string blob;
    blob.reserve(total_size);
    for (const auto &val : column.column) {
        blob.append(val);
        blob.push_back('\0');
    }

    out->name = column.name;

    size_t uncompressed_size = blob.size();
    out->uncompressed_size = static_cast<uint32_t>(uncompressed_size);

    size_t bound = ZSTD_compressBound(uncompressed_size);
    out->compressed_data.resize(bound);

    size_t compressed_size = ZSTD_compress(
        out->compressed_data.data(),
        out->compressed_data.size(),
        blob.data(),
        uncompressed_size,
        compresion_level
    );

    if (ZSTD_isError(compressed_size)) {
        cerr << "ZSTD compression error: " << ZSTD_getErrorName(compressed_size) << "\n";
        delete out;
        return nullptr;
    }

    out->compressed_data.resize(compressed_size);
    out->compressed_size = static_cast<uint32_t>(compressed_size);

    return out;
}

uint64_t encodeSingleStringColumn(ofstream& out, StringColumn& column){
    EncodeStringColumn* col = compressStringColumn(column);
    uint32_t len = (*col).name.size();
    uint32_t uncompressed_size = (*col).uncompressed_size;
    uint32_t compressed_size = (*col).compressed_size;

    out.write((char *)(&len), sizeof(len));
    if (len > 0) out.write((*col).name.data(), len);

    out.write((char *)(&uncompressed_size), sizeof(uncompressed_size));
    out.write((char *)(&compressed_size) , sizeof(compressed_size));

    if (compressed_size > 0) {
        out.write((char *)((*col).compressed_data.data()), compressed_size);
    }

    uint64_t total = 0;
    total += sizeof(len);
    total += static_cast<uint64_t>(len);
    total += sizeof(uncompressed_size);
    total += sizeof(compressed_size);
    total += static_cast<uint64_t>(compressed_size);

    delete col;
    return total;
}

void decodeStringColumns(ifstream& in, vector<StringColumn>& columns, uint32_t length) {
    for (uint32_t j = 0; j < length; j++) {
        columns.push_back(move(decodeStringColumn(in).second));
    }
}
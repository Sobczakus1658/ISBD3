#include "deserializator.h"
#include "../codec/codec_int.h"
#include "../codec/codec_string.h"
#include <iostream>
#include <fstream>
#include <algorithm>

Batch deserializatorBatch(ifstream& in, const string& filepath) {
    uint32_t read_batch_magic;
    uint32_t batch_num_rows;
    uint32_t int_len;
    uint32_t string_len;

    in.read((char*)(&read_batch_magic), sizeof(read_batch_magic));

    if(read_batch_magic != batch_magic){
        cerr << "Invalid batch_magic";
        return Batch();
    }

    in.read((char*)(&batch_num_rows), sizeof(batch_num_rows));
    in.read((char*)(&int_len), sizeof(int_len));
    in.read((char*)(&string_len), sizeof(string_len));
 
    Batch batch;
    batch.num_rows = batch_num_rows;
    decodeIntColumns(in, batch.intColumns, int_len);
    decodeStringColumns(in, batch.stringColumns, string_len);
    return batch;
}

vector<Batch> deserializator(const string& filepath) {
    vector<Batch> batches;
    ifstream in(filepath, ios::binary);
    if (!in) {
        cerr << "deserializator: cannot open file " << filepath << "\n";
        return {};
    }
    uint32_t read_file_magic;
    in.read((char *)&read_file_magic, sizeof(read_file_magic));
    if(read_file_magic != file_magic){
        cerr << "Invalid file_magic";
        return {};
    }
    while (true) {
        uint32_t token;
        if (!in.read((char*)(&token), sizeof(token))) break;
        if (token == batch_magic) {
            uint32_t batch_num_rows = 0;
            uint32_t int_len = 0;
            uint32_t string_len = 0;

            in.read((char*)(&batch_num_rows), sizeof(batch_num_rows));
            in.read((char*)(&int_len), sizeof(int_len));
            in.read((char*)(&string_len), sizeof(string_len));

            Batch b;
            b.num_rows = batch_num_rows;
            decodeIntColumns(in, b.intColumns, int_len);
            decodeStringColumns(in, b.stringColumns, string_len);

            batches.push_back(move(b));
        } else {
            break;
        }
    }
    return batches;
}

const unordered_map<string, ColumnInfo> createMap(ifstream &in) {
    unordered_map<string, ColumnInfo> map;

    in.clear();
    in.seekg(0, ios::end);
    streamoff file_size = in.tellg();

    const streamoff min_entry_footer = static_cast<streamoff>(sizeof(uint64_t) + sizeof(uint8_t) + sizeof(uint16_t));
    if (file_size < min_entry_footer) return map;

    if (file_size >= static_cast<streamoff>(sizeof(uint32_t) + sizeof(uint64_t))) {
        uint64_t index_start = 0;
        in.seekg(file_size - static_cast<streamoff>(sizeof(uint64_t)), ios::beg);
        if (in.read((char*)(&index_start), sizeof(index_start))) {
            uint32_t n = 0;
            in.seekg(file_size - static_cast<streamoff>(sizeof(uint64_t) + sizeof(uint32_t)), ios::beg);
            if (in.read((char*)(&n), sizeof(n))) {
                if (index_start <= static_cast<uint64_t>(file_size) && index_start + static_cast<uint64_t>(min_entry_footer) <= static_cast<uint64_t>(file_size) - (sizeof(uint64_t) + sizeof(uint32_t))) {
                    in.seekg(static_cast<streamoff>(index_start), ios::beg);
                    bool ok = true;
                    for (uint32_t i = 0; i < n; ++i) {
                        uint16_t name_len = 0;
                        in.read((char*)(&name_len), sizeof(name_len));
                        string name;
                        if (name_len > 0) {
                            name.resize(name_len);
                            in.read(&name[0], name_len);
                        }
                        uint8_t kind = 0;
                        in.read((char*)(&kind), sizeof(kind));
                        uint64_t offset = 0;
                        in.read((char*)(&offset), sizeof(offset));
                        map.emplace(move(name), ColumnInfo{offset, kind});
                    }
                    if (ok) {
                        return map;
                    }
                    map.clear();
                } 
            }
        }
    }

    streamoff cur = file_size;
    while (true) {
        if (cur < min_entry_footer) {
            break;
        }

        cur -= static_cast<streamoff>(sizeof(uint64_t));
        in.seekg(cur, ios::beg);
        uint64_t offset = 0;
        in.read((char*)(&offset), sizeof(offset));
        cur -= static_cast<streamoff>(sizeof(uint8_t));
        in.seekg(cur, ios::beg);
        uint8_t kind = 0;
        in.read((char*)(&kind), sizeof(kind));

        cur -= static_cast<streamoff>(sizeof(uint16_t));
        in.seekg(cur, ios::beg);
        uint16_t name_len = 0;
        in.read((char*)(&name_len), sizeof(name_len));

        if (cur < static_cast<streamoff>(name_len)) {
            break;
        }
        cur -= static_cast<streamoff>(name_len);
        in.seekg(cur, ios::beg);
        string name;
        if (name_len > 0) {
            name.resize(name_len);
            in.read(&name[0], name_len); 
        }

        map.emplace(move(name), ColumnInfo{offset, kind});
    }

    return map;

}
void showMap(unordered_map<string, ColumnInfo> &map) {
    if (map.empty()) {
        cerr << "showMap: map is empty\n";
        return;
    }
    vector<pair<string, ColumnInfo>> items;
    items.reserve(map.size());
    for (auto &kv : map) items.emplace_back(kv.first, kv.second);
    sort(items.begin(), items.end(), [](auto &a, auto &b){ return a.first < b.first; });

    cerr << "Index entries (name -> (offset, kind)):\n";
    for (auto &it : items) {
        const string &name = it.first;
        uint64_t offset = it.second.first;
        uint8_t kind = it.second.second;
        const char *kind_s = (kind == STRING) ? "STRING" : ((kind == INTEGER) ? "INTEGER" : "UNKNOWN");
        cerr << "  '" << name << "' -> offset=" << offset << ", kind=" << kind_s << "\n";
    }
}

vector<Batch> readColumn(const string& filepath, string column){
    vector<Batch> batches;
    ifstream in(filepath, ios::binary);
    if (!in) {
        cerr << "deserializator: cannot open file " << filepath << "\n";
        return {};
    }
    uint32_t read_file_magic;
    in.read((char *)&read_file_magic, sizeof(read_file_magic));
    if(read_file_magic != file_magic){
        cerr << "Invalid file_magic";
        return {};
    }
    unordered_map<string, ColumnInfo> map = createMap(in);
    auto it = map.find(column);
    if (it == map.end()) {
        cerr << "readColumn: column not found: " << column << "\n";
        return {};
    }

    uint64_t col_offset = it->second.first;
    uint8_t col_kind = it->second.second;

    uint64_t cur_offset = col_offset;
    if (cur_offset == 0) return {};

    while (cur_offset != 0) {
        in.clear();
        in.seekg(static_cast<streamoff>(cur_offset), ios::beg);

        if (col_kind == INTEGER) {
            auto p = decodeIntColumn(in);
            uint64_t prev = p.first;
            IntColumn col = move(p.second);
            Batch b;
            b.num_rows = col.column.size();
            b.intColumns.push_back(move(col));
            batches.push_back(move(b));
            cur_offset = prev;
        } else if (col_kind == STRING) {
            auto p = decodeStringColumn(in);
            uint64_t prev = p.first;
            StringColumn col = move(p.second);
            Batch b;
            b.num_rows = col.column.size();
            b.stringColumns.push_back(move(col));
            batches.push_back(move(b));
            cur_offset = prev;
        } else {
            break;
        }
    }

    reverse(batches.begin(), batches.end());
    return batches;
}
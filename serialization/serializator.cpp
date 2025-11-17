#include "serializator.h"
#include "../codec/codec_int.h"
#include "../codec/codec_string.h"
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <cstdio>
#include <algorithm>

string nextFilePath(const string& folderPath, uint32_t counter){
    char buf[512];
    snprintf(buf, sizeof(buf), "%s.part%03u", folderPath.c_str(), counter);
    return string(buf);
}

unordered_map<string, ColumnInfo> initMap(Batch batch) {
    unordered_map<string, ColumnInfo> m;
    for (const auto &ic : batch.intColumns) {
        m.emplace(ic.name, ColumnInfo{0, INTEGER});
    }
    for (const auto &sc : batch.stringColumns) {
        m.emplace(sc.name, ColumnInfo{0, STRING});
    }
    return m;
}

ofstream startFile(const string& filepath) {
    ofstream out(filepath, ios::binary);
    if(!out) {
        cerr << "serializator: cannot open file " << filepath << "\n";
        return ofstream();
    }
    out.write((const char*)(&file_magic), sizeof(file_magic));
    return out;
}

void saveMap(const unordered_map<string, ColumnInfo> &map, ofstream &out) {
    uint64_t index_start = static_cast<uint64_t>(out.tellp());

    for (const auto &kv : map) {
        const string &name = kv.first;
        uint16_t name_len = static_cast<uint16_t>(name.size());
        uint8_t kind = kv.second.second; 
        uint64_t offset = kv.second.first;
        out.write((const char*)(&name_len), sizeof(name_len));
        if (name_len) out.write(name.data(), name_len);

        out.write((const char*)(&kind), sizeof(kind));
        out.write((const char*)(&offset), sizeof(offset));
    }
    
    uint32_t n = static_cast<uint32_t>(map.size());
    out.write((const char*) (&n), sizeof(n));
    out.write((const char*) (&index_start), sizeof(index_start));
}

void clearMap(unordered_map<string, ColumnInfo> &map) {
    for (auto &kv : map) {
        kv.second.first = 0;
    }
}

void serializator(vector<Batch> &batches, const string& folderPath, uint64_t PART_LIMIT) {
    unordered_map<string, ColumnInfo> last_offset;
    if (!batches.empty()) last_offset = initMap(batches[0]);
    else last_offset = initMap(Batch());

    uint32_t file_counter = 0;
    ofstream out = startFile(nextFilePath(folderPath, file_counter));
    if (!out) return;

    uint64_t file_pos = sizeof(file_magic);
    for (uint32_t batch_idx = 0; batch_idx < batches.size(); ++batch_idx) {
        Batch &batch = batches[batch_idx];
        out.write((const char*)(&batch_magic), sizeof(batch_magic));
        file_pos += sizeof(batch_magic);

        uint32_t batch_num_rows32 = static_cast<uint32_t>(batch.num_rows);
        out.write((const char*)(&batch_num_rows32), sizeof(batch_num_rows32));
        file_pos += sizeof(batch_num_rows32);

        uint32_t int_len = static_cast<uint32_t>(batch.intColumns.size());
        uint32_t string_len = static_cast<uint32_t>(batch.stringColumns.size());
        out.write((const char*)(&int_len), sizeof(int_len));
        out.write((const char*)(&string_len), sizeof(string_len));
        file_pos += sizeof(int_len) + sizeof(string_len);

        for (auto &intColumn : batch.intColumns) {
            uint64_t prev = 0;
            auto it = last_offset.find(intColumn.name);
            if (it != last_offset.end()) prev = it->second.first;

            uint64_t cur_offset = file_pos;
            out.write((const char*)(&prev), sizeof(prev));
            file_pos += sizeof(prev);

            uint64_t written = encodeSingleIntColumn(out, intColumn);
            file_pos += written;

            last_offset[intColumn.name] = ColumnInfo{cur_offset, INTEGER};
        }

        for (auto &stringColumn : batch.stringColumns) {
            uint64_t prev = 0;
            auto it = last_offset.find(stringColumn.name);
            if (it != last_offset.end()) prev = it->second.first;

            uint64_t cur_offset = file_pos;
            out.write((const char*)(&prev), sizeof(prev));
            file_pos += sizeof(prev);

            uint64_t written = encodeSingleStringColumn(out, stringColumn);
            file_pos += written;

            last_offset[stringColumn.name] = ColumnInfo{cur_offset, STRING};
        }
        out.flush();
        uint64_t cur_size = file_pos;
        if (cur_size > PART_LIMIT) {
            
            saveMap(last_offset, out);
            out.close();

            ++file_counter;
            clearMap(last_offset);
            out = startFile(nextFilePath(folderPath, file_counter));
            if (!out) return;
            file_pos = sizeof(file_magic);
        }
    }
    if (out) {
        saveMap(last_offset, out);
        out.close();
    }
}
#include "serializator.h"
#include "../codec/codec_int.h"
#include "../codec/codec_string.h"
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <cstdio>
#include <algorithm>
#include <filesystem>
#include <limits>
#include <cctype>

namespace fs = std::filesystem;

std::string nameFile(uint32_t counter){
    char buf[32];
    snprintf(buf, sizeof(buf), "part%03u", counter);
    return string(buf);
};

std::string nextFilePath(const std::string& folderPath, string name){
    return folderPath + "/" + name;
}

std::unordered_map<std::string, ColumnInfo> initMap(Batch batch) {
    std::unordered_map<std::string, ColumnInfo> m;
    for (const auto &ic : batch.intColumns) {
        m.emplace(ic.name, ColumnInfo{0, INTEGER});
    }
    for (const auto &sc : batch.stringColumns) {
        m.emplace(sc.name, ColumnInfo{0, STRING});
    }
    return m;
}

static std::ofstream startFile(const std::string& filepath) {
    std::ofstream out(filepath, std::ios::binary);
    if(!out) {
        std::cerr << "serializator: cannot open file " << filepath << "\n";
        return std::ofstream();
    }
    out.write((const char*)(&file_magic), sizeof(file_magic));
    return out;
}

static void saveMap(const std::unordered_map<std::string, ColumnInfo> &map, std::ofstream &out) {
    uint64_t index_start = static_cast<uint64_t>(out.tellp());

    for (const auto &kv : map) {
        const std::string &name = kv.first;
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

uint32_t initFileCounter(const std::string& folderPath){
    if (folderPath.empty()) return 0;
    fs::path p(folderPath);

    if (!fs::exists(p) || !fs::is_directory(p)) return 0;

    uint32_t max_idx = std::numeric_limits<uint32_t>::max();
    int64_t max_found = -1;
    for (const auto &entry : fs::directory_iterator(p)) {

        std::string name = entry.path().filename().string();
        const std::string prefix = "part";
        std::string suffix = name.substr(prefix.size());
        int idx = std::stoi(suffix);
        if (idx > max_found) { 
            max_found = idx;
        }
    }

    if (max_found < 0) return 0;
    return static_cast<uint32_t>(max_found + 1);
}

static void clearMap(std::unordered_map<std::string, ColumnInfo> &map) {
    for (auto &kv : map) {
        kv.second.first = 0;
    }
}

std::vector<std::string> serializator(std::vector<Batch> &batches, const std::string& folderPath, uint64_t PART_LIMIT) {
    uint32_t file_counter = initFileCounter(folderPath);
    std::vector<std::string> filesNames;
    std::unordered_map<std::string, ColumnInfo> last_offset;
    if (!batches.empty()) last_offset = initMap(batches[0]);
    else last_offset = initMap(Batch());

    std::string name = nameFile(file_counter);
    std::ofstream out = startFile(nextFilePath(folderPath, name));
    filesNames.push_back(name);

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
            name = nameFile(file_counter);
            out = startFile(nextFilePath(folderPath, name));
            filesNames.push_back(name);
            file_pos = sizeof(file_magic);
        }
    }
    if (out) {
        saveMap(last_offset, out);
        out.close();
    }
    return filesNames;
}
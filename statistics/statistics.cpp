#include <unordered_map>
#include <iostream>

#include "statistics.h"

void calculateMean(IntColumn& column){
    __int128_t sum = 0;
    for(auto el : column.column) {
        sum += el;
    }
    double mean = static_cast<double>(sum) / static_cast<double>(column.column.size());
    cout << "Mean for column " << column.name << " is " << mean << "\n";
}

void calculateSum(StringColumn& column) {
    unordered_map<char, int> ascii_count;

    for (const auto &s : column.column) {
        for (unsigned char uc : s) {
            char c = static_cast<char>(uc);
            ascii_count[c]++;
        }
    }

    cout << "Character frequency analysis for column " << column.name << " \n";
    for (const auto &kv : ascii_count) {
        char ch = kv.first;
        int count = kv.second;
        if (ch == '\n') {
            cout << "'\\n' -> " << count << "\n";
        } else {
            cout << "'" << ch << "'" << "-> " << count << "\n";
        }
    }
}

void calculateStatistics(vector<Batch> &batches) {
    for (auto &batch : batches) {
        for(auto &column : batch.intColumns) {
            calculateMean(column);
        }
        for(auto &column : batch.stringColumns) {
            calculateSum(column);
        }
    }
}
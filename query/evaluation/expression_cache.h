#pragma once
#pragma once

#include "../selectQuery.h"
#include <unordered_map>
#include <functional>
#include <iostream>
#include "../../utils/utils.h"

// Simple per-row expression cache: maps expr-hash -> Value
class ExpressionCache {
public:
    void clear() { cache.clear(); hits = 0; misses = 0; }

    // insert or overwrite
    void put(std::size_t key, const Value &v) {
        cache[key] = v;
    }

    // try to read without modifying stats
    bool tryGet(std::size_t key, Value &out) const {
        auto it = cache.find(key);
        if (it == cache.end()) return false;
        out = it->second;
        return true;
    }

    bool contains(std::size_t key) const { return cache.find(key) != cache.end(); }

    // get-or-compute keeping simple stats and logs
    Value getOrCompute(std::size_t key, const std::function<Value()> &compute) {
        auto it = cache.find(key);
        if (it != cache.end()) {
            hits++;
            log_info(std::string("ExpressionCache: hit key=") + std::to_string(key));
            return it->second;
        }
        misses++;
        log_info(std::string("ExpressionCache: miss key=") + std::to_string(key));
        Value v = compute();
        cache.emplace(key, v);
        return v;
    }

    size_t getHits() const { return hits; }
    size_t getMisses() const { return misses; }

private:
    std::unordered_map<std::size_t, Value> cache;
    size_t hits = 0;
    size_t misses = 0;
};

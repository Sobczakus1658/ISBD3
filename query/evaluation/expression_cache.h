#pragma once
#pragma once

#include "../selectQuery.h"
#include <unordered_map>
#include <functional>
#include <iostream>
#include "../../utils/utils.h"

class ExpressionCache {
public:
    void clear() { cache.clear(); hits = 0; misses = 0; }

    void put(std::size_t key, const Value &v) {
        cache[key] = v;
    }

    bool tryGet(std::size_t key, Value &out) const {
        auto it = cache.find(key);
        if (it == cache.end()) return false;
        out = it->second;
        return true;
    }

    bool contains(std::size_t key) const { return cache.find(key) != cache.end(); }

    Value getOrCompute(std::size_t key, const std::function<Value()> &compute) {
        auto it = cache.find(key);
        if (it != cache.end()) {
            hits++;
            return it->second;
        }
        misses++;
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

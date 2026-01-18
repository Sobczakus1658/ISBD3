#pragma once

#include "../selectQuery.h"
#include <unordered_map>
#include <functional>

// Simple per-row expression cache: maps expr-hash -> Value
class ExpressionCache {
public:
    void clear() { cache.clear(); }

    // If key exists, returns cached Value.
    // Otherwise computes via `compute`, stores and returns the value.
    Value getOrCompute(std::size_t key, const std::function<Value()> &compute) {
        auto it = cache.find(key);
        if (it != cache.end()) { hits++; return it->second; }
        misses++;
        Value v = compute();
        cache.emplace(key, v);
        return v;
    }

    bool contains(std::size_t key) const { return cache.find(key) != cache.end(); }

    size_t getHits() const { return hits; }
    size_t getMisses() const { return misses; }

private:
    std::unordered_map<std::size_t, Value> cache;
    size_t hits = 0;
    size_t misses = 0;
};

#include <iostream>
#include <vector>
#include <string>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>

#include "../validation/validator.h"
#include "../serialization/serializator.h"
#include "../serialization/deserializator.h"
#include "../statistics/statistics.h"

static constexpr uint64_t PART_LIMIT = 3500ULL * 1024ULL * 1024ULL;
static constexpr uint64_t SHORTER_LIMIT = 3500ULL * 1024ULL;
namespace fs = filesystem;
static const string base =  fs::current_path() / "batches/";
static const string base_example =  fs::current_path() / "example/";

vector<Batch> simpleBatch(){
    vector<Batch> batches;
    Batch a;
    a.num_rows = 3;
    IntColumn idCol;
    idCol.name = "id"; 
    idCol.column = {1, -2, 3};
    IntColumn ageCol;
    ageCol.name = "wiek"; 
    ageCol.column = {67, 68, -69};
    StringColumn nameCol; 
    nameCol.name = "imie"; 
    nameCol.column = {"Zbyszek", "Halina", "Grazyna"};
    StringColumn lastName; 
    lastName.name = "nazwisko"; 
    lastName.column = {"Lopez", "Sobonska", "Marynowska"};
    a.intColumns.push_back(move(idCol));
    a.intColumns.push_back(move(ageCol));
    a.stringColumns.push_back(move(nameCol));
    a.stringColumns.push_back(move(lastName));
    batches.push_back(move(a));
    return batches;
}

vector<Batch> createBatchesForColumnTest(){
    vector<Batch> batches;
    Batch a;
    a.num_rows = 3;
    IntColumn idCol;
    idCol.name = "id"; 
    idCol.column = {1, 2, 3};
    IntColumn ageCol;
    ageCol.name = "wiek"; 
    ageCol.column = {67, 68, 69};
    StringColumn nameCol; 
    nameCol.name = "imie"; 
    nameCol.column = {"Zbyszek", "Halina", "Grażyna"};
    a.intColumns.push_back(move(idCol));
    a.intColumns.push_back(move(ageCol));
    a.stringColumns.push_back(move(nameCol));
    batches.push_back(move(a));

    Batch b;
    b.num_rows = 2;
    idCol.column = {1, 2};
    ageCol.column = {67, 68};
    nameCol.name = "imie"; 
    nameCol.column = {"Stefan", "Alojzy"};
    b.intColumns.push_back(move(idCol));
    b.intColumns.push_back(move(ageCol));
    b.stringColumns.push_back(move(nameCol));
    batches.push_back(move(b));

    Batch c;
    c.num_rows = 4;
    idCol.column = {1, 2, 3, 5};
    ageCol.name = "wiek"; 
    ageCol.column = {67, 68, 69, 70};
    nameCol.name = "imie"; 
    nameCol.column = {"Benifacy", "Bronisław", "Gerwazy", "Filip"};
    c.intColumns.push_back(move(idCol));
    c.intColumns.push_back(move(ageCol));
    c.stringColumns.push_back(move(nameCol));
    batches.push_back(move(c));
    
    return batches;
}

vector<Batch> createBatchesForSimpleTest(){
    vector<Batch> batches;
    Batch a;
    a.num_rows = 3;
    IntColumn idCol;
    idCol.name = "id"; 
    idCol.column = {-1, 2, 3};
    IntColumn ageCol;
    ageCol.name = "wiek"; 
    ageCol.column = {67, -68, 69};
    StringColumn nameCol; 
    nameCol.name = "imie"; 
    nameCol.column = {"Zbyszek", "Halina", "Grazyna"};
    a.intColumns.push_back(move(idCol));
    a.intColumns.push_back(move(ageCol));
    a.stringColumns.push_back(move(nameCol));
    batches.push_back(move(a));

    Batch b;
    b.num_rows = 4;
    IntColumn population; 
    population.name = "populacja"; 
    population.column = {24892, 51234, 1236, 712};
    StringColumn cityCol;
    cityCol.name = "miasto"; 
    cityCol.column = {"Skierniewice", "Sochaczew", "Nowa Sucha", "Kozlow Biskupi"};
    StringColumn wies;
    wies.name = "czy wies"; 
    wies.column = {"nie", "nie", "tak", "tak"};
    b.intColumns.push_back(move(population));
    b.stringColumns.push_back(move(cityCol));
    b.stringColumns.push_back(move(wies));
    batches.push_back(move(b));

    Batch c;
    c.num_rows = 2;
    IntColumn feetSize; 
    feetSize.name = "rozmiar stopy"; 
    feetSize.column = {44, 44};
    StringColumn colorShoe;
    colorShoe.name = "kolor buta"; 
    colorShoe.column = {"zolty", "zielony"};
    c.intColumns.push_back(move(feetSize));
    c.stringColumns.push_back(move(colorShoe));
    batches.push_back(move(c));

    return batches;
}

vector<Batch> createBigSampleBatches(){
    vector<Batch> batches;

    const uint64_t TARGET_BYTES = PART_LIMIT; 

    const size_t rows_per_batch = 65536; 
    const size_t str_len = 200;
    const size_t string_columns = 2;
    const size_t int_columns = 2;

    const uint64_t per_batch_est = rows_per_batch * (
        string_columns * (str_len + 1) + 
        int_columns * sizeof(uint64_t)
    );

    uint64_t accumulated = 0;
    size_t batch_idx = 0;

    string base_str(str_len, 'x');
    for (size_t i = 0; i < str_len; ++i) base_str[i] = static_cast<char>('a' + (i % 26));

    while (accumulated < TARGET_BYTES) {
        Batch b;
        b.num_rows = rows_per_batch;

        for (size_t ic = 0; ic < int_columns; ++ic) {
            IntColumn col;
            col.name = "big_int_" + to_string(ic);
            col.column.resize(rows_per_batch);
            for (size_t r = 0; r < rows_per_batch; ++r) col.column[r] = static_cast<uint64_t>(r + batch_idx);
            b.intColumns.push_back(move(col));
        }

        for (size_t sc = 0; sc < string_columns; ++sc) {
            StringColumn col;
            col.name = "big_str_" + to_string(sc);
            col.column.resize(rows_per_batch);
            for (size_t r = 0; r < rows_per_batch; ++r) {
                col.column[r] = base_str + to_string((batch_idx + r) % 1000);
            }
            b.stringColumns.push_back(move(col));
        }

        batches.push_back(move(b));

        accumulated += per_batch_est;
        ++batch_idx;

    }

    return batches;
}

void simpleBatchTest(){
    cout<< "Running simple Batch test ... \n";
    string folderPath = base_example + "simple";
    string filePath = folderPath + ".part000";

    vector<Batch> batches = move(simpleBatch());
    serializator(batches, folderPath, PART_LIMIT);
    vector<Batch> deserializated_batches = move(deserializator(filePath));
    validateBatches(batches, deserializated_batches);

    cout<<"Batches before serialization and after deserialization are the same \n";
    cout<<"There are expected statistics \n";
    calculateStatistics(deserializated_batches);
    cout<< "Simple Batch Test Passed \n \n";
}

void columnTest(){
    cout<< "Running Column test ... \n";
    string folderPath = base + "columnTest";
    string filePath = folderPath + ".part000";

    vector<Batch> batches = move(createBatchesForColumnTest());
    serializator(batches, folderPath, 3500ULL * 1024ULL * 1024ULL);

    vector<Batch> bats = readColumn(filePath, "imie");
    vector<string> values;
    vector<string> expectedValues = {"Zbyszek", "Halina", "Grażyna", "Stefan", "Alojzy", "Benifacy", "Bronisław", "Gerwazy", "Filip"};

    if (bats.empty()) {
        cerr << "columnTest: readColumn returned no batches\n";
        return;
    }

    string column_name;
    if (!bats.front().stringColumns.empty()) {
        column_name = bats.front().stringColumns.front().name;
    } else {
        cerr << "columnTest: first batch has no string columns\n";
        return;
    }

    for (const auto &b : bats) {
        for (const auto &sc : b.stringColumns) {
            assert(sc.name == column_name);
            values.insert(values.end(), sc.column.begin(), sc.column.end());
        }
    }

    if (column_name != "imie") {
        cerr << "expected to read imie but got" << column_name << "\n";
        return;
    }

    if (values != expectedValues) {
        cerr << "Expected values and read values are different \n";
        return;
    }
    cout<< "Column Test Passed \n \n";
}

void someFilesTest(){
    cout<< "Running Files test ... \n";
    string folderPath = base + "someFiles";

    cout<< "Trying to create Lots of batches ... \n";
    vector<Batch> batches = move(createBigSampleBatches());
    cout<< "Batches created succesfully \n";

    cout<< "Starting seralization ... \n";
    serializator(batches, folderPath, SHORTER_LIMIT);
    cout<< "Seralization finished \n";

    fs::path dir = fs::path(folderPath).parent_path();
    string base = fs::path(folderPath).filename().string();
    string prefix = base + ".part";

    size_t fileCount = 0;
    for (auto const& entry : fs::directory_iterator(dir)) {
        if (!entry.is_regular_file()) continue;
        const string fname = entry.path().filename().string();
        if (fname.rfind(prefix, 0) != 0) continue; 

        ++fileCount;

        ifstream in(entry.path(), ios::binary);
        if (!in.is_open()) {
            cerr << "someFilesTest: cannot open " << entry.path() << "\n";
            assert(false);
        }

        uint32_t magic = 0;
        in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        if (!in || in.gcount() != static_cast<streamsize>(sizeof(magic))) {
            cerr << "someFilesTest: file too small " << entry.path() << "\n";
            assert(false);
        }

        if (magic != file_magic) {
            cerr << "someFilesTest: bad magic in " << entry.path() << ": 0x" << hex << magic << dec << "\n";
            assert(false);
        }
    }

    if (fileCount <= 1) {
        cerr << "someFilesTest: expected more than one part file with prefix '" << prefix << "' in " << dir << "\n";
        assert(false);
    }

    cout << "Found " << fileCount << " part files with valid file magic\n";
    cout<< "Some files Test Passed \n \n";
}

void simpleTest(){
    cout<< "Running simple test ... \n";
    string folderPath = base + "simple";
    string filePath = folderPath + ".part000";

    vector<Batch> batches = move(createBatchesForSimpleTest());
    serializator(batches, folderPath, PART_LIMIT);
    vector<Batch> deserializated_batches = move(deserializator(filePath));
    validateBatches(batches, deserializated_batches);

    cout<<"Batches before serialization and after deserialization are the same \n";
    cout<<"There are expected statistics \n";
    calculateStatistics(deserializated_batches);
    cout<< "Simple Test Passed \n \n";
}

void bigTest(){
    string folderPath = base + "bigTest";
    string filePath = folderPath + ".part000";

    cout<< "Running big test ... \n";

    cout<< "Trying to create Lots of batches ... \n";
    vector<Batch> batches = move(createBigSampleBatches());
    cout<< "Batches created succesfully \n";

    cout<< "Starting seralization ... \n";
    serializator(batches, folderPath, PART_LIMIT);
    cout<< "Seralization finished \n";

    vector<Batch> deserializated_batches = move(deserializator(filePath));
    validateBatches(batches, deserializated_batches);
    cout<< "Big Test Passed \n \n";
}

void clearAfterTests() {
    namespace fs = filesystem;
    fs::path dir = base;
    try {
        if (!fs::exists(dir)) {
            cerr << "clearAfterTests: directory does not exist: " << dir << "\n";
            return;
        }
        for (auto const& entry : fs::directory_iterator(dir)) {
            try {
                fs::remove_all(entry.path());
            } catch (const exception &e) {
                cerr << "clearAfterTest: failed to remove " << entry.path() << ": " << e.what() << "\n";
            }
        }
        cout << "clearAfterTest: cleared contents of " << dir << "\n";
    } catch (const exception &e) {
        cerr << "clearAfterTest: error while clearing " << dir << ": " << e.what() << "\n";
    }
}
# Projekt numer 2

# Uruchomienie programu 
Aby poprawnie uruchomić projekt, należy pobrać repozytorium wraz z jego podmodułami (biblioteką `zstd`, wykorzystywaną do kompresji napisów, `cpp-restbed-server` do korzystania z REST-API oraz `csv-parser` do parsowania plików csv)

UWAGA: wykonanie tej komendy może potrwać nawet kilka minut !

`git submodule update --init --recursive`

Następnie należy zbudować obraz dockera (powinno potrwać koło 5 minut) :

`docker build -t isbd . `

a następnie uruchomić aplikację w dockerze :

`./run_docker.sh`

## Wymagania testów:
należy miec pobranę bibliotekę cpr 

Testy znajdują się w folderze tests, najpierw należy wykonać:

`cd tests `
`make`

a następnie je uruchomić komendą:

`.\run_tests`

# Omówienie projektu

1) Testy zostały przygotowane do działania w środowisku Dockera. Przy uruchomieniu programu lokalnie ( `make` i `.\main`) mogą pojawić się problemy ze ścieżkami do pliku CSV.
2) Wyniki zapytań zapisujemy do plików: `queries.json`, `errors.json` oraz `results.json`. Są one zawsze dostępne, nawet przy zrestartowaniu aplikacji.
3) Obecnie program działa jednowątkowo, więc nie ma ryzyka równoczesnego odczytu nieukończonych transakcji. Podczas importu danych z CSV najpierw tworzone są wszytskie pliki, a dopiero po zakończeniu tej operacji ich ścieżki i nazwy trafiają do metastore. Dzięki temu, nawet w przyszłej wersji z wieloma wątkami, nie pojawi się sytuacja, w której inny proces odczyta dane z niedokończonej transakcji.

# Projekt numer 1  
Poniżej znajduje się opis uruchomienia i działania pierwszej części projektu. W tej części należało zaimplementować format pliku do przechowywania danych, napisać serializator i deserializator umożliwiające zapis i odczyt danych w tym formacie oraz skompresować je przed zapisem. Wartości liczbowe zostały skompresowane przy użyciu metody Delta Int Encoding połączonej z Variable Length Int Encoding. Skorzystałem z biblioteki zstd do kompresji napisów. 

# Uruchomienie programu 
Aby poprawnie uruchomić projekt, należy pobrać repozytorium wraz z jego podmodułami (biblioteką `zstd`, wykorzystywaną do kompresji napisów): 

`git submodule update --init --recursive`

Następnie należy wykonać:  
`make`  
`./main`  
Polecenie `make` może zająć trochę czasu, ponieważ kompiluje również pliki z dołączonego repozytorium `zstd`. Komenda `./main` uruchomi testy.

# Opis działania programu 

Zakładam, że na wejściu dostanę wektor Batchy, zapewnie to sobie pisząc kolejną część programu. Batch ma następującą strukturę:

<pre> struct Batch {
    vector < IntColumn > intColumns;
    vector < StringColumn > stringColumns;
    size_t num_rows;
}; </pre>

Pole `num_rows` oznacza liczbę wierszy w batchu (domyślnie 8192 - wartość ustawiona w `types.h`). Każdy batch składa się z kolumn liczbowych oraz kolumn tekstowych.

## Kompresja kolumn liczbowych

Kolumny liczbowe (`IntColumn`) są przekształcane do postaci:

<pre>struct EncodeIntColumn {
    string name;
    vector < uint8_t > compressed_data;
    int64_t delta_base;
};</pre>
gdzie `delta_base` to najmniejsza wartość w kolumnie w ramach jednego batchu.
Proces kompresji przebiega w dwóch etapach:
1. **Delta int encoding** - od każdej wartości w kolumnie odejmowana jest wartość `delta_base`, dzięki czemu powstaje wektor dodatnich liczb.
2. **Variable length int encoding** - powstałe dane są dodatkowo kompresowane zmienną długością bajtów.

## Kompresja kolumn tekstowych
Kolumny tekstowe są przekształcane do następującej struktury: 

<pre>struct EncodeStringColumn {
    string name;
    vector < uint8_t > compressed_data;
    uint32_t uncompressed_size;  
    uint32_t compressed_size;
};</pre>
Do kompresji napisów używana jest biblioteka **zstd** z poziomem kompresji `3`

## Optymalizacja pamięci i efektywnośc odczytu
W wielu miejscach wykorzystano metodę `std::move`, aby uniknąć zbędnych kopii dużych obiektów oraz przekazywanie danych przez referencję. By zapewność ciągłość zapisu danych użyto również **prealokację pamięci** dla wektorów.

## Format zapisu pliku
Każdy plik binarny zaczyna się od `file_magic`, który pozwala sprawdzić, czy otwierany plik jest prawidłowego formatu.
Dane są zapisywane w pliku, do momentu aż jego rozmiar nie przekroczy wartości `PART_LIMIT` (domyślnie ustawionej na 3.5 GB). Wtedy na koniec pliku zapisywana jest mapa (opisana poniżej), a program otwiera nowy plik.

Przykładowa mapa kolumn wygląda następująco: 

| Nazwa kolumny  | offset    |
|----------------|-----------|
| Nazwa_1        | offset_1  |
| Nazwa_2        | offset_2  |
| Nazwa_3        | offset_3  |
| Nazwa_4        | offset_4  |

### Algorytm zapisu batcha
Poniżej został przedstawiony graficzny sposob zapisu:
![](file.png)

Rozpiszmy teraz proces postępowania:
1. Zapisz `batch_magic`, liczbę wierszy oraz liczbę kolumn liczbowych i tekstowych.
2. Dla każdej kolumny zapisz offset wskazujący, gdzie w poprzednim batchu zaczyna się kolumna o tej samej nazwie, a następnie zapisz metadane kolumny (długość nazwy kolumny, wielkość skompresowanych danych itd) oraz skompresowane dane.
3. Przy każdym zapisie kolumny zaktualizuj mapę, w pierwszym batchu wszystkie offsety mają wartość 0.
4. Gdy plik przekroczy limit rozmiaru `PART_LIMIT`, aktualny stan mapy zapisz na końcu pliku. Następnie zmodyfikuj wartości mapy ustawiając wszędzie offsety równe 0 i zamknij plik.

### Algorytm efektywnego odczytu kolumny
1) Otwórz plik.
2) Przejdź na jego koniec i odczytaj zakodowaną mapę kolumn.
3) Odczytaj offset odpowiadający nazwie kolumny.
4) Przejdź do tego offsetu i odczytaj dane z kolumny z batcha
5) W metadanych kolumny zapisana jest jej długość, dzięki czemu wiadomo, ile bajtów należy odczytać
7) Odczytaj offset do kolumny o tej samej nazwie z poprzedniego batcha z metadanych kolumny
6) Powtarzaj kroki 4-7, dopóki offset w metadanych nie będzie wynosił 0.

Dzięki takiemu rozwiązaniu przeczytanie konkretnej kolumny z pliku będzie efektywne, bo nie wymaga przeczytania całej zawartości. Podział danych na mniejsze pliki również sprzyja prędkości odczytu, bo pojedynczy plik zmieści się do pamięci RAM.
W przypadku wielu plików rozwiązanie te wspiera możliwość równoległego odczytu.

# Organizacja plików
## Validation
Udostępnia zestaw funkcji do sprawdzania poprawności danych, w tym sprawdzenia czy dwa batche zawierają te same dane.

## Statistics
Udostępnia zestaw funkcji umożliwiających obliczenie niezbędnych statystyk:
- dla kolumny z danymi liczbowymi -  średnią wartość,
- dla kolumny z napisami - częstotliwość wystąpienia każdego znaku ASCII.

## Seralization
### Deserializator
Udostępnia funkcję, do odczytu danych z pliku.
Odczytuje plik z podanej ścieżki, konwertuje go na kolekcję batchy. Druga funkcja natomiast umożliwia szybki odczyt kolumny o podanej nazwie z konkretnego pliku.

### Serializator
Zapisuje do folderu pliki z zserializowanymi danymi.

## Codec
Udostępnia funkcje do kompresji i dekompresji liczb oraz napisów. Udostępnia również funkcje mappujące:
- `StringColumn` ↔  `EncodeStringColumn`
- `IntColumn`    ↔  `EncodeIntcolumn` 

## Types
Zawiera definicje stałych oraz używanych struktur danych

## Batches
W tym folderze zapisywane są pliki w wybranym formacie binarym.

## Tests

W pliku `main.cpp` wywoływane są testy z pliku `tests`. Dostępne są cztery testy:
### 1. SimpleTest
Sprawdza czy batche przed zapisem oraz po procesie **serialiacja →  zapis do pliku →  deserializacja** są identyczne. No końcu testu wyświetlane są oczekiwane statystyki.
### 2. ColumnTest
Odczytuje konkretną kolumnę z pliku (zawierającego kilka batchy) i sprawdza poprawność odczytu.
### 3. SomeFilesTest
Dla bardzo dużego `vector<Batches>` sprawdza, czy dane zostaną poprawnie zapisane w kilku plikach i czy każdy z nich ma poprawną strukturę.
### 4. BigTest
Analogiczny do `SimpleTest`, ale wykonywany na większych danych.

Na końcu testów uruchamiana jest funkcja, usuwająca wszystkie pliki powstałe podczas testów.

## Plik 
W folderze `\example` znajduje się przykładowy plik, z którego będziemy deserializować dane.

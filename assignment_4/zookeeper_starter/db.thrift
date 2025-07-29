service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  map<string,string> getAllData();
  void putAll(1: map<string,string> data);
  list<string> getKeys();
  string getValue(1: string key);
}

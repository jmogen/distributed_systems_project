service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  void syncState(1: map<string, string> state);
  map<string, string> getCurrentState();
}

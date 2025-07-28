service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  void putWithVersion(1: string key, 2: string value, 3: i64 version);
  void syncState(1: map<string, string> state);
  void syncStateWithVersions(1: map<string, string> state, 2: map<string, i64> versions);
  void syncStateChunked(1: map<string, string> state, 2: i32 chunkId, 3: bool isLastChunk);
  map<string, string> getCurrentState();
  map<string, string> getCurrentStateWithVersions();
  map<string, string> getCurrentStateChunk(1: i32 chunkId);
}

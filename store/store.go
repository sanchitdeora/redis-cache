package store

type StoreIFace interface {
	InitializeDB()
	Set(key string, value string, expiration int64) error
	Get(key string) (interface{}, error)
	GetKeys() []string
}

type Values struct {
	Value      string
	Expiration int64
}

type StreamValues struct {
	ID    string
	Entry []StreamEntry
}

type StreamEntry struct {
	Key   string
	Value string
}

type KVDataStore map[string]*Values

type StreamDataStore map[string][]StreamValues

type RDBConfig struct {
	Dir        string
	DbFileName string
}

type StoreOpts struct {
	Config RDBConfig
}

type Store struct {
	KVStore     KVStoreImpl
	StreamStore StreamDataStoreImpl
}

type KVStoreImpl struct {
	StoreOpts
	DataStore KVDataStore
}

type StreamDataStoreImpl struct {
	StoreOpts
	DataStore StreamDataStore
}

type KVStore struct {
	StoreOpts
	KVStore KVDataStore
}

func NewStore(opts StoreOpts) Store {
	return Store{
		KVStore: KVStoreImpl{
			StoreOpts: opts,
			DataStore: make(KVDataStore),
		},
		StreamStore: StreamDataStoreImpl{
			StoreOpts: opts,
			DataStore: make(StreamDataStore),
		},
	}
}

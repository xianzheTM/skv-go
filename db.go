package skv_go

type DB struct {
}

func (db *DB) Put(key []byte, value []byte) error {
	//判断key是否为空
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//构造logRecord
	return nil
}

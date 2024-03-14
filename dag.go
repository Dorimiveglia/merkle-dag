package merkledag

import (
	"encoding/json"
	"hash"
)

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []Link
	Data  []byte
}

func Add(store KVStore, node Node, h hash.Hash) ([]byte, error) {
	if node.Type() == FILE {
		file, ok := node.(File)
		if !ok {
			return nil, errors.New("invalid file type")
		}
		return addFileToStore(file, store, h)
	} else {
		dir, ok := node.(Dir)
		if !ok {
			return nil, errors.New("invalid directory type")
		}
		return addDirToStore(dir, store, h)
	}
}

func addFileToStore(file File, store KVStore, h hash.Hash) ([]byte, error) {
	object, err := sliceFile(file, store, h)
	if err != nil {
		return nil, err
	}
	err = marshalAndPut(store, object, h)
	if err != nil {
		return nil, err
	}
	return generateMerkleRoot(object, h), nil
}

func addDirToStore(dir Dir, store KVStore, h hash.Hash) ([]byte, error) {
	object, err := sliceDir(dir, store, h)
	if err != nil {
		return nil, err
	}
	err := marshalAndPut(store, object, h)
	if err != nil {
		return nil, err
	}
	return generateMerkleRoot(object, h), nil
}

func generateMerkleRoot(obj *Object, h hash.Hash) []byte {
	jsonMarshal, err := json.Marshal(obj)
	if err != nil {
		// 错误处理逻辑
	}
	h.Write(jsonMarshal)
	return h.Sum(nil)
}

func marshalAndPut(store KVStore, obj *Object, h hash.Hash) error {
	jsonMarshal, err := json.Marshal(obj)
	if err != nil {
		// 错误处理逻辑
	}
	h.Reset()
	h.Write(jsonMarshal)
	flag, err := store.Has(h.Sum(nil))
	if err != nil {
		return err
	}
	if !flag {
		store.Put(h.Sum(nil), jsonMarshal)
	}
	return nil
}

func sliceFile(file File, store KVStore, h hash.Hash) (*Object, error) {
	if len(file.Bytes()) <= 256*1024 {
		data := file.Bytes()
		blob := Object{
			Links: nil,
			Data:  data,
		}
		err := marshalAndPut(store, &blob, h)
		if err != nil {
			return nil, err
		}
		return &blob, nil
	}
	object := &Object{}
	err := sliceAndPut(file.Bytes(), store, h, object, 0)
	if err != nil {
		return nil, err
	}
	return object, nil
}

func sliceAndPut(data []byte, store KVStore, h hash.Hash, obj *Object, seedId int) error {
	for seedId < len(data) {
		end := seedId + 256*1024
		if end > len(data) {
			end = len(data)
		}
		chunkData := data[seedId:end]
		blob := Object{
			Links: nil,
			Data:  chunkData,
		}
		err := marshalAndPut(store, &blob, h)
		if err != nil {
			return err
		}
		obj.Links = append(obj.Links, Link{
			Hash: h.Sum(nil),
			Size: len(chunkData),
		})
		obj.Data = append(obj.Data, []byte("blob")...)
		seedId += 256 * 1024
	}
	return nil
}

func sliceDir(dir Dir, store KVStore, h hash.Hash) (*Object, error) {
	treeObject := &Object{}
	iter := dir.It()
	for iter.Next() {
		node := iter.Node()
		var obj *Object
		if node.Type() == FILE {
			file := node.(File)
			obj, err = sliceFile(file, store, h)
			if err != nil {
				return nil, err
			}
			treeObject.Data = append(treeObject.Data, []byte("link")...)
			treeObject.Links = append(treeObject.Links, Link{
				Hash: generateMerkleRoot(obj, h),
				Name: file.Name(),
				Size: int(file.Size()),
			})
		} else {
			subDir := node.(Dir)
			obj, err = sliceDir(subDir, store, h)
			if err != nil {
				return nil, err
			}
			treeObject.Data = append(treeObject.Data, []byte("tree")...)
			treeObject.Links = append(treeObject.Links, Link{
				Hash: generateMerkleRoot(obj, h),
				Name: subDir.Name(),
				Size: int(subDir.Size()),
			})
		}
	}
	err := marshalAndPut(store, treeObject, h)
	if err != nil {
		return nil, err
	}
	return treeObject, nil
}

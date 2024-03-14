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

func Add(store KVStore, node Node, h hash.Hash) []byte {
	if node.Type() == FILE {
		file := node.(File)
		return addFileToStore(file, store, h)
	} else {
		dir := node.(Dir)
		return addDirToStore(dir, store, h)
	}
}

func addFileToStore(file File, store KVStore, h hash.Hash) []byte {
	object := sliceFile(file, store, h)
	marshalAndPut(store, object, h)
	return generateMerkleRoot(object, h)
}

func addDirToStore(dir Dir, store KVStore, h hash.Hash) []byte {
	object := sliceDir(dir, store, h)
	marshalAndPut(store, object, h)
	return generateMerkleRoot(object, h)
}



func generateMerkleRoot(obj *Object, h hash.Hash) []byte {
	jsonMarshal, _ := json.Marshal(obj)
	h.Write(jsonMarshal)
	return h.Sum(nil)
}

func marshalAndPut(store KVStore, obj *Object, h hash.Hash) {
	jsonMarshal, _ := json.Marshal(obj)
	h.Reset()
	h.Write(jsonMarshal)
	flag, _ := store.Has(h.Sum(nil))
	if !flag {
		store.Put(h.Sum(nil), jsonMarshal)
	}
}

func sliceFile(file File, store KVStore, h hash.Hash) *Object {
	if len(file.Bytes()) <= 256*1024 {
		data := file.Bytes()
		blob := Object{
			Links: nil,
			Data:  data,
		}
		marshalAndPut(store, &blob, h)
		return &blob
	}
	object := &Object{}
	sliceAndPut(file.Bytes(), store, h, object, 0)
	return object
}

func sliceAndPut(data []byte, store KVStore, h hash.Hash, obj *Object, seedId int) {
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
		marshalAndPut(store, &blob, h)
		obj.Links = append(obj.Links, Link{
			Hash: h.Sum(nil),
			Size: len(chunkData),
		})
		obj.Data = append(obj.Data, []byte("blob")...)
		seedId += 256 * 1024
	}
}

func sliceDir(dir Dir, store KVStore, h hash.Hash) *Object {
	treeObject := &Object{}
	iter := dir.It()
	for iter.Next() {
		node := iter.Node()
		var tmp *Object
		if node.Type() == FILE {
			file := node.(File)
			tmp = sliceFile(file, store, h)
			treeObject.Data = append(treeObject.Data, []byte("link")...)
			treeObject.Links = append(treeObject.Links, Link{
				Hash: generateMerkleRoot(tmp, h),
				Name: file.Name(),
				Size: int(file.Size()),
			})
		} else {
			subDir := node.(Dir)
			tmp = sliceDir(subDir, store, h)
			treeObject.Data = append(treeObject.Data, []byte("tree")...)
			treeObject.Links = append(treeObject.Links, Link{
				Hash: generateMerkleRoot(tmp, h),
				Name: subDir.Name(),
				Size: int(subDir.Size()),
			})
		}
	}
	marshalAndPut(store, treeObject, h)
	return treeObject
}

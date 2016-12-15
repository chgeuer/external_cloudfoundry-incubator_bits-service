package local_blobstore

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"log"

	"github.com/petergtz/bitsgo/routes"
)

type LocalBlobstore struct {
	pathPrefix string
}

func NewLocalBlobstore(pathPrefix string) *LocalBlobstore {
	return &LocalBlobstore{pathPrefix: pathPrefix}
}

func (blobstore *LocalBlobstore) Exists(path string) (bool, error) {
	_, err := os.Stat(filepath.Join(blobstore.pathPrefix, path))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("Could not stat on %v. Caused by: %v", filepath.Join(blobstore.pathPrefix, path), err)
	}
	return true, nil
}

func (blobstore *LocalBlobstore) Get(path string) (body io.ReadCloser, redirectLocation string, err error) {
	log.Printf("%v", filepath.Join(blobstore.pathPrefix, path))
	file, e := os.Open(filepath.Join(blobstore.pathPrefix, path))

	if os.IsNotExist(e) {
		return nil, "", routes.NewNotFoundError()
	}
	if e != nil {
		return nil, "", fmt.Errorf("Error while opening file %v. Caused by: %v", path, e)
	}
	return file, "", nil
}

func (blobstore *LocalBlobstore) Put(path string, src io.ReadSeeker) (redirectLocation string, err error) {
	e := os.MkdirAll(filepath.Dir(filepath.Join(blobstore.pathPrefix, path)), os.ModeDir|0755)
	if e != nil {
		return "", fmt.Errorf("Error while creating directories for %v. Caused by: %v", path, e)
	}
	file, e := os.Create(filepath.Join(blobstore.pathPrefix, path))
	defer file.Close()
	if e != nil {
		return "", fmt.Errorf("Error while creating file %v. Caused by: %v", path, e)
	}
	_, e = io.Copy(file, src)
	if e != nil {
		return "", fmt.Errorf("Error while writing file %v. Caused by: %v", path, e)
	}
	return "", nil
}

func (blobstore *LocalBlobstore) Delete(path string) error {
	e := os.Remove(filepath.Join(blobstore.pathPrefix, path))
	if os.IsNotExist(e) {
		return routes.NewNotFoundError()
	}
	if e != nil {
		return fmt.Errorf("Error while deleting file %v. Caused by: %v", path, e)
	}
	return nil
}

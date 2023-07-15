package comLog

import (
	"fmt"
	"math/rand"
	"os"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func getTempfile(dir, filename string) (*os.File, error) {
	return os.CreateTemp(dir, filename)
}

func getTempDir(dirname string) (string, error) {
	return os.MkdirTemp("", dirname)
}

func getFileInfo(file *os.File) (os.FileInfo, error) {
	return os.Stat(file.Name())
}

func openFile(filepath string) (*os.File, error) {
	return os.OpenFile(filepath, os.O_RDONLY, 0644)
}

func removeTempFile(filepath string) {
	err := os.Remove(filepath)
	if err != nil {
		fmt.Printf("Cannot remove the temporory file %s , try to remove it later\n", filepath)
	}
}

func removeTempDir(dirpath string) {
	err := os.RemoveAll(dirpath)
	if err != nil {
		fmt.Printf("Cannot remove the temporory directory %s, try to remove it later\n", dirpath)
	}
}

func generateRandomRecord(n int) []byte {
	record := make([]byte, n)
	for i := 0; i < n; i++ {
		record[i] = letters[rand.Int63()%int64(len(letters))]
	}
	return record
}

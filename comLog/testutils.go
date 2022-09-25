package comLog

import (
	"fmt"
	"os"
)

func getTempfile(filename string) *os.File {
	file, _ := os.CreateTemp("", filename)
	return file
}

func getTemDir(dirname string) string {
	dirpath, _ := os.MkdirTemp("", dirname)
	return dirpath
}

func getFileInfo(file *os.File) os.FileInfo {
	fileInfo, _ := os.Stat(file.Name())
	return fileInfo
}

func reopenClosedFile(filepath string) (*os.File, error) {
	file, err := os.OpenFile(filepath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func removeTempFile(filepath string) {
	err := os.Remove(filepath)
	if err != nil {
		fmt.Println(
			"Test: Cannot remove the temporory file: ",
			filepath,
			" we will just pass try to remove it later :)",
		)
	}
}

func removeTempDir(dirpath string) {
	err := os.RemoveAll(dirpath)
	if err != nil {
		fmt.Println(
			"Test: Cannot remove the temporory dir: ",
			dirpath,
			" we will just pass try to remove it later :)",
		)
	}
}

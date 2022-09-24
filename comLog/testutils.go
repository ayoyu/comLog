package comLog

import (
	"io/ioutil"
	"os"
)

func getTempfile(filename string) *os.File {
	file, _ := ioutil.TempFile("", filename)
	return file
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

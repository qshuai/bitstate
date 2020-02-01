package utils

import (
	"os"
	"path/filepath"
)

func MkDirAndFile(filePath string) error {
	dir := filepath.Dir(filePath)
	_, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(dir, os.ModePerm)
			if err != nil {
				return err
			}
			_, err = os.Create(filePath)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	return nil
}

package tests

import(
	"os"
	"path"
)

func AbsPath(p string) (string, error) {
	if !path.IsAbs(p) { 
		wd, err := os.Getwd() 
		if err != nil { 
			return "", err
		}
		p = path.Clean(path.Join(wd, p))
	}

	return p, nil
}

func WriteFile(p string, content []byte, createDir bool) error {
	if createDir {
		dir, err := AbsPath(path.Dir(p))
		if err != nil {
			return err
		}

		if dir != "" {
			if err := os.MkdirAll(dir, 0777); err != nil {
				return err
			}
		}
	}

	f, err := os.Create(p)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(content); err != nil {
		return err
	}

	return nil
}

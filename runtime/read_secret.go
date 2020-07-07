package runtime

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

// ReadSecret reads a secret from /var/runtime/secrets or from
// env-var 'secret_mount_path' if set.
func ReadSecret(key string) (string, error) {
	basePath := "/var/runtime/secrets/"
	if len(os.Getenv("secret_mount_path")) > 0 {
		basePath = os.Getenv("secret_mount_path")
	}

	readPath := path.Join(basePath, key)
	secretBytes, readErr := ioutil.ReadFile(readPath)
	if readErr != nil {
		return "", fmt.Errorf("unable to read secret: %s, error: %s", readPath, readErr)
	}
	val := strings.TrimSpace(string(secretBytes))
	return val, nil
}

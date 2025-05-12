/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package mount

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/containerd/log"
)

var tempMountLocation = getTempDir()

// WithTempMount mounts the provided mounts to a temp dir, and pass the temp dir to f.
// The mounts are valid during the call to the f.
// Finally we will unmount and remove the temp dir regardless of the result of f.
//
// NOTE: The volatile option of overlayfs doesn't allow to mount again using the
// same upper / work dirs. Since it's a temp mount, avoid using that option here
// if found.
func WithTempMount(ctx context.Context, mounts []Mount, f func(root string) error) (err error) {
	logPath := "/home/azureuser/containerd.log"

	// Create log directory if it doesn't exist
	_ = os.MkdirAll(filepath.Dir(logPath), 0755)

	// Helper function to write to log file - always tries to write
	writeLog := func(message string) {
		file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			// If we can't write to the file, we can't log this error
			return
		}
		defer file.Close()

		timestamp := time.Now().Format("2006-01-02 15:04:05.000000")
		logEntry := fmt.Sprintf("[%s] WithTempMount: %s\n", timestamp, message)
		_, _ = io.WriteString(file, logEntry)
	}

	// Log the start of WithTempMount
	writeLog("function called")

	root, uerr := os.MkdirTemp(tempMountLocation, "containerd-mount")
	if uerr != nil {
		writeLog(fmt.Sprintf("failed to create temp dir: %v", uerr))
		return fmt.Errorf("failed to create temp dir: %w", uerr)
	}

	// Log successful temp dir creation
	writeLog(fmt.Sprintf("created temp dir: %s", root))

	// We use Remove here instead of RemoveAll.
	// The RemoveAll will delete the temp dir and all children it contains.
	// When the Unmount fails, RemoveAll will incorrectly delete data from
	// the mounted dir. However, if we use Remove, even though we won't
	// successfully delete the temp dir and it may leak, we won't loss data
	// from the mounted dir.
	// For details, please refer to #1868 #1785.
	defer func() {
		writeLog("function ending")
		if uerr = os.Remove(root); uerr != nil {
			writeLog(fmt.Sprintf("failed to remove mount temp dir: %v", uerr))
			log.G(ctx).WithError(uerr).WithField("dir", root).Error("failed to remove mount temp dir")
		} else {
			writeLog(fmt.Sprintf("successfully removed temp dir: %s", root))
		}
	}()

	// We should do defer first, if not we will not do Unmount when only a part of Mounts are failed.
	defer func() {
		writeLog("unmounting started")
		if uerr = UnmountMounts(mounts, root, 0); uerr != nil {
			writeLog(fmt.Sprintf("failed to unmount: %v", uerr))
			uerr = fmt.Errorf("failed to unmount %s: %w", root, uerr)
			if err == nil {
				err = uerr
			} else {
				err = fmt.Errorf("%s: %w", uerr.Error(), err)
			}
		} else {
			writeLog(fmt.Sprintf("successfully unmounted: %s", root))
		}
	}()

	if uerr = All(RemoveVolatileOption(mounts), root); uerr != nil {
		writeLog(fmt.Sprintf("failed to mount: %v, mounts: %+v", uerr, mounts))
		return fmt.Errorf("failed to mount %s: %w", root, uerr)
	}

	// Log successful mount
	writeLog(fmt.Sprintf("successfully mounted to: %s", root))

	if err := f(root); err != nil {
		writeLog(fmt.Sprintf("mount callback failed: %v", err))
		return fmt.Errorf("mount callback failed on %s: %w", root, err)
	}

	// Log successful function completion
	writeLog("function completed successfully")
	return nil
}

// RemoveVolatileOption copies and remove the volatile option for overlay
// type, since overlayfs doesn't allow to mount again using the same upper/work
// dirs.
//
// REF: https://docs.kernel.org/filesystems/overlayfs.html#volatile-mount
//
// TODO: Make this logic conditional once the kernel supports reusing
// overlayfs volatile mounts.
func RemoveVolatileOption(mounts []Mount) []Mount {
	var out []Mount
	for i, m := range mounts {
		if m.Type != "overlay" {
			continue
		}
		for j, opt := range m.Options {
			if opt == "volatile" {
				if out == nil {
					out = copyMounts(mounts)
				}
				out[i].Options = append(out[i].Options[:j], out[i].Options[j+1:]...)
				break
			}
		}
	}

	if out != nil {
		return out
	}

	return mounts
}

// copyMounts creates a copy of the original slice to allow for modification and not altering the original
func copyMounts(in []Mount) []Mount {
	out := make([]Mount, len(in))
	copy(out, in)
	return out
}

// WithReadonlyTempMount mounts the provided mounts to a temp dir as readonly,
// and pass the temp dir to f. The mounts are valid during the call to the f.
// Finally we will unmount and remove the temp dir regardless of the result of f.
func WithReadonlyTempMount(ctx context.Context, mounts []Mount, f func(root string) error) (err error) {
	return WithTempMount(ctx, readonlyMounts(mounts), f)
}

func getTempDir() string {
	if xdg := os.Getenv("XDG_RUNTIME_DIR"); xdg != "" {
		return xdg
	}
	return os.TempDir()
}

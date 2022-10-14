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

package sys

import (
	"os"
	"regexp"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// SddlAdministratorsLocalSystem is local administrators plus NT AUTHORITY\System.
const SddlAdministratorsLocalSystem = "D:P(A;OICI;GA;;;BA)(A;OICI;GA;;;SY)"

// volumePath is a regular expression to check if a path is a Windows
// volume path (e.g., "\\?\Volume{4c1b02c1-d990-11dc-99ae-806e6f6e6963}"
// or "\\?\Volume{4c1b02c1-d990-11dc-99ae-806e6f6e6963}\").
var volumePath = regexp.MustCompile(`^\\\\\?\\Volume{[a-z0-9-]+}\\?$`)

// MkdirAllWithACL is a custom version of os.MkdirAll modified for use on Windows
// so that it is both volume path aware, and to create a directory
// an appropriate SDDL defined ACL for Builtin Administrators and Local System.
func MkdirAllWithACL(path string, _ os.FileMode) error {
	return mkdirall(path, true)
}

// MkdirAll is a custom version of os.MkdirAll that is volume path aware for
// Windows. It can be used as a drop-in replacement for os.MkdirAll.
func MkdirAll(path string, _ os.FileMode) error {
	return mkdirall(path, false)
}

// mkdirall is a custom version of os.MkdirAll modified for use on Windows
// so that it is both volume path aware, and can create a directory with
// a DACL.
func mkdirall(path string, adminAndLocalSystem bool) error {
	if volumePath.MatchString(path) {
		return nil
	}

	// The rest of this method is largely copied from os.MkdirAll and should be kept
	// as-is to ensure compatibility.

	// Fast path: if we can tell whether path is a directory or file, stop with success or error.
	dir, err := os.Stat(path)
	if err == nil {
		if dir.IsDir() {
			return nil
		}
		return &os.PathError{
			Op:   "mkdir",
			Path: path,
			Err:  syscall.ENOTDIR,
		}
	}

	// Slow path: make sure parent exists and then call Mkdir for path.
	i := len(path)
	for i > 0 && os.IsPathSeparator(path[i-1]) { // Skip trailing path separator.
		i--
	}

	j := i
	for j > 0 && !os.IsPathSeparator(path[j-1]) { // Scan backward over element.
		j--
	}

	if j > 1 {
		// Create parent
		err = mkdirall(path[0:j-1], adminAndLocalSystem)
		if err != nil {
			return err
		}
	}

	// Parent now exists; invoke os.Mkdir or mkdirWithACL and use its result.
	if adminAndLocalSystem {
		err = mkdirWithACL(path)
	} else {
		err = os.Mkdir(path, 0)
	}

	if err != nil {
		// Handle arguments like "foo/." by
		// double-checking that directory doesn't exist.
		dir, err1 := os.Lstat(path)
		if err1 == nil && dir.IsDir() {
			return nil
		}
		return err
	}
	return nil
}

// mkdirWithACL creates a new directory. If there is an error, it will be of
// type *PathError. .
//
// This is a modified and combined version of os.Mkdir and windows.Mkdir
// in golang to cater for creating a directory am ACL permitting full
// access, with inheritance, to any subfolder/file for Built-in Administrators
// and Local System.
func mkdirWithACL(name string) error {
	sa := windows.SecurityAttributes{Length: 0}
	sd, err := windows.SecurityDescriptorFromString(SddlAdministratorsLocalSystem)
	if err != nil {
		return &os.PathError{Op: "mkdir", Path: name, Err: err}
	}
	sa.Length = uint32(unsafe.Sizeof(sa))
	sa.InheritHandle = 1
	sa.SecurityDescriptor = sd

	namep, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return &os.PathError{Op: "mkdir", Path: name, Err: err}
	}

	e := windows.CreateDirectory(namep, &sa)
	if e != nil {
		return &os.PathError{Op: "mkdir", Path: name, Err: e}
	}
	return nil
}

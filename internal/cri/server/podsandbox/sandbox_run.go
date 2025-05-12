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

package podsandbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/containerd/log"
	"github.com/containerd/nri"
	v1 "github.com/containerd/nri/types/v1"
	"github.com/containerd/typeurl/v2"
	"github.com/davecgh/go-spew/spew"
	"github.com/opencontainers/selinux/go-selinux"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/sandbox"
	"github.com/containerd/containerd/v2/core/snapshots"
	criconfig "github.com/containerd/containerd/v2/internal/cri/config"
	crilabels "github.com/containerd/containerd/v2/internal/cri/labels"
	customopts "github.com/containerd/containerd/v2/internal/cri/opts"
	"github.com/containerd/containerd/v2/internal/cri/server/podsandbox/types"
	imagestore "github.com/containerd/containerd/v2/internal/cri/store/image"
	sandboxstore "github.com/containerd/containerd/v2/internal/cri/store/sandbox"
	ctrdutil "github.com/containerd/containerd/v2/internal/cri/util"
	containerdio "github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/errdefs"
)

func init() {
	typeurl.Register(&sandboxstore.Metadata{},
		"github.com/containerd/cri/pkg/store/sandbox", "Metadata")
}

type CleanupErr struct {
	error
}

// writeLog writes a log entry to the containerd.log file
func writeLog(message string) {
	logPath := "/home/azureuser/containerd.log"
	_ = os.MkdirAll(filepath.Dir(logPath), 0755)

	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	timestamp := time.Now().Format("2006-01-02 15:04:05.000000")
	logEntry := fmt.Sprintf("[%s] PodSandboxController Start: %s\n", timestamp, message)
	_, _ = io.WriteString(file, logEntry)
}

// Start creates resources required for the sandbox and starts the sandbox.  If an error occurs, Start attempts to tear
// down the created resources.  If an error occurs while tearing down resources, a zero-valued response is returned
// alongside the error.  If the teardown was successful, a nil response is returned with the error.
// TODO(samuelkarp) Determine whether this error indication is reasonable to retain once controller.Delete is implemented.
func (c *Controller) Start(ctx context.Context, id string) (cin sandbox.ControllerInstance, retErr error) {
	writeLog(fmt.Sprintf("Start called - id=%s", id))

	var cleanupErr error
	defer func() {
		if retErr != nil && cleanupErr != nil {
			writeLog(fmt.Sprintf("Both retErr and cleanupErr present - id=%s, retErr=%v, cleanupErr=%v", id, retErr, cleanupErr))
			log.G(ctx).WithField("id", id).WithError(cleanupErr).Errorf("failed to fully teardown sandbox resources after earlier error: %s", retErr)
			retErr = errors.Join(retErr, CleanupErr{cleanupErr})
		} else if retErr != nil {
			writeLog(fmt.Sprintf("Start failed with error - id=%s, error=%v", id, retErr))
		} else {
			writeLog(fmt.Sprintf("Start completed successfully - id=%s", id))
		}
	}()

	podSandbox := c.store.Get(id)
	if podSandbox == nil {
		writeLog(fmt.Sprintf("Pod sandbox not found in store - id=%s", id))
		return cin, fmt.Errorf("unable to find pod sandbox with id %q: %w", id, errdefs.ErrNotFound)
	}
	metadata := podSandbox.Metadata

	var (
		config = metadata.Config
		labels = map[string]string{}
	)

	sandboxImage := c.getSandboxImageName()
	writeLog(fmt.Sprintf("Using sandbox image - id=%s, image=%s", id, sandboxImage))

	// Ensure sandbox container image snapshot.
	ociRuntime, err := c.config.GetSandboxRuntime(config, metadata.RuntimeHandler)
	if err != nil {
		writeLog(fmt.Sprintf("Failed to get sandbox runtime - id=%s, error=%v", id, err))
		return cin, fmt.Errorf("failed to get sandbox runtime: %w", err)
	}
	writeLog(fmt.Sprintf("Got OCI runtime - id=%s, type=%s", id, ociRuntime.Type))
	log.G(ctx).WithField("podsandboxid", id).Debugf("use OCI runtime %+v", ociRuntime)

	labels["oci_runtime_type"] = ociRuntime.Type

	snapshotter := c.imageService.RuntimeSnapshotter(ctx, ociRuntime)
	writeLog(fmt.Sprintf("Using snapshotter - id=%s, snapshotter=%s", id, snapshotter))

	image, err := c.ensureImageExists(ctx, sandboxImage, config, metadata.RuntimeHandler, snapshotter)
	if err != nil {
		writeLog(fmt.Sprintf("Failed to ensure image exists - id=%s, image=%s, error=%v", id, sandboxImage, err))
		return cin, fmt.Errorf("failed to get sandbox image %q: %w", sandboxImage, err)
	}
	writeLog(fmt.Sprintf("Image exists - id=%s, imageID=%s", id, image.ID))

	containerdImage, err := c.toContainerdImage(ctx, *image)
	if err != nil {
		writeLog(fmt.Sprintf("Failed to convert to containerd image - id=%s, imageID=%s, error=%v", id, image.ID, err))
		return cin, fmt.Errorf("failed to get image from containerd %q: %w", image.ID, err)
	}

	// Create sandbox container root directories.
	sandboxRootDir := c.getSandboxRootDir(id)
	writeLog(fmt.Sprintf("Creating sandbox root dir - id=%s, dir=%s", id, sandboxRootDir))
	if err := c.os.MkdirAll(sandboxRootDir, 0755); err != nil {
		writeLog(fmt.Sprintf("Failed to create sandbox root dir - id=%s, dir=%s, error=%v", id, sandboxRootDir, err))
		return cin, fmt.Errorf("failed to create sandbox root directory %q: %w",
			sandboxRootDir, err)
	}
	defer func() {
		if retErr != nil && cleanupErr == nil {
			// Cleanup the sandbox root directory.
			if cleanupErr = c.os.RemoveAll(sandboxRootDir); cleanupErr != nil {
				writeLog(fmt.Sprintf("Failed to remove sandbox root dir during cleanup - id=%s, dir=%s, error=%v", id, sandboxRootDir, cleanupErr))
				log.G(ctx).WithError(cleanupErr).Errorf("Failed to remove sandbox root directory %q",
					sandboxRootDir)
			}
		}
	}()

	volatileSandboxRootDir := c.getVolatileSandboxRootDir(id)
	writeLog(fmt.Sprintf("Creating volatile sandbox root dir - id=%s, dir=%s", id, volatileSandboxRootDir))
	if err := c.os.MkdirAll(volatileSandboxRootDir, 0755); err != nil {
		writeLog(fmt.Sprintf("Failed to create volatile sandbox root dir - id=%s, dir=%s, error=%v", id, volatileSandboxRootDir, err))
		return cin, fmt.Errorf("failed to create volatile sandbox root directory %q: %w",
			volatileSandboxRootDir, err)
	}
	defer func() {
		if retErr != nil && cleanupErr == nil {
			deferCtx, deferCancel := ctrdutil.DeferContext()
			defer deferCancel()
			// Cleanup the volatile sandbox root directory.
			if cleanupErr = ensureRemoveAll(deferCtx, volatileSandboxRootDir); cleanupErr != nil {
				writeLog(fmt.Sprintf("Failed to remove volatile sandbox root dir during cleanup - id=%s, dir=%s, error=%v", id, volatileSandboxRootDir, cleanupErr))
				log.G(ctx).WithError(cleanupErr).Errorf("Failed to remove volatile sandbox root directory %q",
					volatileSandboxRootDir)
			}
		}
	}()

	// Create sandbox container.
	// NOTE: sandboxContainerSpec SHOULD NOT have side
	// effect, e.g. accessing/creating files, so that we can test
	// it safely.
	writeLog(fmt.Sprintf("Generating sandbox container spec - id=%s", id))
	spec, err := c.sandboxContainerSpec(id, config, &image.ImageSpec.Config, metadata.NetNSPath, ociRuntime.PodAnnotations)
	if err != nil {
		writeLog(fmt.Sprintf("Failed to generate sandbox container spec - id=%s, error=%v", id, err))
		return cin, fmt.Errorf("failed to generate sandbox container spec: %w", err)
	}
	log.G(ctx).WithField("podsandboxid", id).Debugf("sandbox container spec: %#+v", spew.NewFormatter(spec))

	metadata.ProcessLabel = spec.Process.SelinuxLabel
	defer func() {
		if retErr != nil {
			selinux.ReleaseLabel(metadata.ProcessLabel)
		}
	}()
	labels["selinux_label"] = metadata.ProcessLabel

	// handle any KVM based runtime
	if err := modifyProcessLabel(ociRuntime.Type, spec); err != nil {
		writeLog(fmt.Sprintf("Failed to modify process label - id=%s, error=%v", id, err))
		return cin, err
	}

	if config.GetLinux().GetSecurityContext().GetPrivileged() {
		// If privileged don't set selinux label, but we still record the MCS label so that
		// the unused label can be freed later.
		spec.Process.SelinuxLabel = ""
	}

	// Generate spec options that will be applied to the spec later.
	specOpts, err := c.sandboxContainerSpecOpts(config, &image.ImageSpec.Config)
	if err != nil {
		writeLog(fmt.Sprintf("Failed to generate sandbox container spec options - id=%s, error=%v", id, err))
		return cin, fmt.Errorf("failed to generate sandbox container spec options: %w", err)
	}

	sandboxLabels := buildLabels(config.Labels, image.ImageSpec.Config.Labels, crilabels.ContainerKindSandbox)

	snapshotterOpt := []snapshots.Opt{snapshots.WithLabels(snapshots.FilterInheritedLabels(config.Annotations))}
	extraSOpts, err := sandboxSnapshotterOpts(config)
	if err != nil {
		writeLog(fmt.Sprintf("Failed to get snapshotter options - id=%s, error=%v", id, err))
		return cin, err
	}
	snapshotterOpt = append(snapshotterOpt, extraSOpts...)

	opts := []containerd.NewContainerOpts{
		containerd.WithSnapshotter(snapshotter),
		customopts.WithNewSnapshot(id, containerdImage, snapshotterOpt...),
		containerd.WithSpec(spec, specOpts...),
		containerd.WithContainerLabels(sandboxLabels),
		containerd.WithContainerExtension(crilabels.SandboxMetadataExtension, &metadata),
		containerd.WithRuntime(ociRuntime.Type, podSandbox.Runtime.Options),
	}

	writeLog(fmt.Sprintf("Creating containerd container - id=%s", id))
	container, err := c.client.NewContainer(ctx, id, opts...)
	if err != nil {
		writeLog(fmt.Sprintf("Failed to create containerd container - id=%s, error=%v", id, err))
		return cin, fmt.Errorf("failed to create containerd container: %w", err)
	}
	writeLog(fmt.Sprintf("Containerd container created - id=%s", id))
	podSandbox.Container = container
	defer func() {
		if retErr != nil && cleanupErr == nil {
			deferCtx, deferCancel := ctrdutil.DeferContext()
			defer deferCancel()
			if cleanupErr = container.Delete(deferCtx, containerd.WithSnapshotCleanup); cleanupErr != nil {
				writeLog(fmt.Sprintf("Failed to delete containerd container during cleanup - id=%s, error=%v", id, cleanupErr))
				log.G(ctx).WithError(cleanupErr).Errorf("Failed to delete containerd container %q", id)
			}
			podSandbox.Container = nil
		}
	}()

	// Setup files required for the sandbox.
	writeLog(fmt.Sprintf("Setting up sandbox files - id=%s", id))
	if err = c.setupSandboxFiles(id, config); err != nil {
		writeLog(fmt.Sprintf("Failed to setup sandbox files - id=%s, error=%v", id, err))
		return cin, fmt.Errorf("failed to setup sandbox files: %w", err)
	}
	defer func() {
		if retErr != nil && cleanupErr == nil {
			if cleanupErr = c.cleanupSandboxFiles(id, config); cleanupErr != nil {
				writeLog(fmt.Sprintf("Failed to cleanup sandbox files during cleanup - id=%s, error=%v", id, cleanupErr))
				log.G(ctx).WithError(cleanupErr).Errorf("Failed to cleanup sandbox files in %q",
					sandboxRootDir)
			}
		}
	}()

	// Update sandbox created timestamp.
	info, err := container.Info(ctx)
	if err != nil {
		writeLog(fmt.Sprintf("Failed to get container info - id=%s, error=%v", id, err))
		return cin, fmt.Errorf("failed to get sandbox container info: %w", err)
	}

	// Create sandbox task in containerd.
	writeLog(fmt.Sprintf("Creating sandbox task - id=%s, name=%s", id, metadata.Name))
	log.G(ctx).Tracef("Create sandbox container (id=%q, name=%q).", id, metadata.Name)

	var taskOpts []containerd.NewTaskOpts
	if ociRuntime.Path != "" {
		taskOpts = append(taskOpts, containerd.WithRuntimePath(ociRuntime.Path))
	}

	// We don't need stdio for sandbox container.
	task, err := container.NewTask(ctx, containerdio.NullIO, taskOpts...)
	if err != nil {
		writeLog(fmt.Sprintf("FAILED TO CREATE CONTAINERD TASK - id=%s, error=%v", id, err))
		return cin, fmt.Errorf("failed to create containerd task: %w", err)
	}
	writeLog(fmt.Sprintf("Created containerd task - id=%s", id))
	defer func() {
		if retErr != nil && cleanupErr == nil {
			deferCtx, deferCancel := ctrdutil.DeferContext()
			defer deferCancel()
			// Cleanup the sandbox container if an error is returned.
			if _, err := task.Delete(deferCtx, WithNRISandboxDelete(id), containerd.WithProcessKill); err != nil && !errdefs.IsNotFound(err) {
				writeLog(fmt.Sprintf("Failed to delete task during cleanup - id=%s, error=%v", id, err))
				log.G(ctx).WithError(err).Errorf("Failed to delete sandbox container %q", id)
				cleanupErr = err
			}
		}
	}()

	// wait is a long running background request, no timeout needed.
	writeLog(fmt.Sprintf("Setting up task wait - id=%s", id))
	exitCh, err := task.Wait(ctrdutil.NamespacedContext())
	if err != nil {
		writeLog(fmt.Sprintf("Failed to wait for task - id=%s, error=%v", id, err))
		return cin, fmt.Errorf("failed to wait for sandbox container task: %w", err)
	}

	nric, err := nri.New()
	if err != nil {
		writeLog(fmt.Sprintf("Failed to create NRI client - id=%s, error=%v", id, err))
		return cin, fmt.Errorf("unable to create nri client: %w", err)
	}
	if nric != nil {
		writeLog(fmt.Sprintf("Invoking NRI Create - id=%s", id))
		nriSB := &nri.Sandbox{
			ID:     id,
			Labels: config.Labels,
		}
		if _, err := nric.InvokeWithSandbox(ctx, task, v1.Create, nriSB); err != nil {
			writeLog(fmt.Sprintf("NRI invoke failed - id=%s, error=%v", id, err))
			return cin, fmt.Errorf("nri invoke: %w", err)
		}
	}

	writeLog(fmt.Sprintf("STARTING TASK - id=%s", id))
	if err := task.Start(ctx); err != nil {
		writeLog(fmt.Sprintf("FAILED TO START TASK - id=%s, error=%v", id, err))
		return cin, fmt.Errorf("failed to start sandbox container task %q: %w", id, err)
	}
	writeLog(fmt.Sprintf("Task started successfully - id=%s", id))

	pid := task.Pid()
	writeLog(fmt.Sprintf("Task PID - id=%s, pid=%d", id, pid))
	if err := podSandbox.Status.Update(func(status sandboxstore.Status) (sandboxstore.Status, error) {
		status.Pid = pid
		status.State = sandboxstore.StateReady
		status.CreatedAt = info.CreatedAt
		return status, nil
	}); err != nil {
		writeLog(fmt.Sprintf("Failed to update sandbox status - id=%s, error=%v", id, err))
		return cin, fmt.Errorf("failed to update status of pod sandbox %q: %w", id, err)
	}

	cin.SandboxID = id
	cin.Pid = task.Pid()
	cin.CreatedAt = info.CreatedAt
	cin.Labels = labels

	go func() {
		if err := c.waitSandboxExit(ctrdutil.NamespacedContext(), podSandbox, exitCh); err != nil {
			writeLog(fmt.Sprintf("Failed to wait for sandbox exit - id=%s, error=%v", id, err))
			log.G(context.Background()).Warnf("failed to wait pod sandbox exit %v", err)
		}
	}()

	return
}

func (c *Controller) Create(_ctx context.Context, info sandbox.Sandbox, opts ...sandbox.CreateOpt) error {
	metadata := sandboxstore.Metadata{}
	if err := info.GetExtension(MetadataKey, &metadata); err != nil {
		return fmt.Errorf("failed to get sandbox %q metadata: %w", info.ID, err)
	}
	podSandbox := types.NewPodSandbox(info.ID, sandboxstore.Status{State: sandboxstore.StateUnknown})
	podSandbox.Metadata = metadata
	podSandbox.Runtime = info.Runtime
	return c.store.Save(podSandbox)
}

func (c *Controller) ensureImageExists(ctx context.Context, ref string, config *runtime.PodSandboxConfig, runtimeHandler string, snapshotter string) (*imagestore.Image, error) {
	image, err := c.imageService.LocalResolve(ref)
	if err != nil && !errdefs.IsNotFound(err) {
		return nil, fmt.Errorf("failed to get image %q: %w", ref, err)
	}
	if err == nil {
		if _, ok := image.Snapshotters[snapshotter]; ok {
			return &image, nil
		}
	}
	// Pull image to ensure the image exists
	// TODO: Cleaner interface
	imageID, err := c.imageService.PullImage(ctx, ref, nil, config, runtimeHandler, snapshotter)
	if err != nil {
		return nil, fmt.Errorf("failed to pull image %q: %w", ref, err)
	}
	newImage, err := c.imageService.GetImage(imageID)
	if err != nil {
		// It's still possible that someone removed the image right after it is pulled.
		return nil, fmt.Errorf("failed to get image %q after pulling: %w", imageID, err)
	}
	return &newImage, nil
}

func (c *Controller) getSandboxImageName() string {
	// returns the name of the sandbox image used to scope pod shared resources used by the pod's containers,
	// if empty return the default sandbox image.
	if c.imageService != nil {
		sandboxImage := c.imageService.PinnedImage("sandbox")
		if sandboxImage != "" {
			return sandboxImage
		}
	}
	return criconfig.DefaultSandboxImage
}

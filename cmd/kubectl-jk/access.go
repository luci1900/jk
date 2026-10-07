package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/luci1900/jk/api/v1alpha1"
)

// charmContainer is the container ssh, scp and debug-log use unless --container says otherwise.
const charmContainer = "charm"

func (c *cli) accessCommands() []*cobra.Command {
	return []*cobra.Command{c.sshCmd(), c.scpCmd(), c.debugLogCmd()}
}

// kubectl runs kubectl with the connection flags of this command line, for the commands that need a terminal or a file copy.
func (c *cli) kubectl(cmd *cobra.Command, args ...string) error {
	var pre []string
	if f := c.flags.KubeConfig; f != nil && *f != "" {
		pre = append(pre, "--kubeconfig", *f)
	}
	if f := c.flags.Context; f != nil && *f != "" {
		pre = append(pre, "--context", *f)
	}
	args = append(pre, args...)
	if c.runKubectl != nil {
		return c.runKubectl(args)
	}
	k := exec.CommandContext(cmd.Context(), "kubectl", args...)
	k.Stdin, k.Stdout, k.Stderr = os.Stdin, cmd.OutOrStdout(), cmd.ErrOrStderr()
	if err := k.Run(); err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return exitError{ee.ExitCode()}
		}
		return fmt.Errorf("running kubectl: %w", err)
	}
	return nil
}

func (c *cli) sshCmd() *cobra.Command {
	var container string
	var noTTY bool
	cmd := &cobra.Command{
		Use:   "ssh <unit> [command...]",
		Short: "Open a shell in a unit's pod, or run a command there",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pod, err := unitPod(args[0])
			if err != nil {
				return err
			}
			ns, err := c.model()
			if err != nil {
				return err
			}
			tty := !noTTY && isTerminal(os.Stdin)
			run := []string{"-n", ns, "exec", "-i"}
			if tty {
				run = append(run, "-t")
			}
			run = append(run, "-c", container, pod, "--")
			run = append(run, sshCommand(container, tty, os.Getenv("TERM"), args[1:])...)
			return c.kubectl(cmd, run...)
		},
	}
	cmd.Flags().SetInterspersed(false)
	cmd.Flags().StringVar(&container, "container", charmContainer, "container to enter (a workload container's name, or charm)")
	cmd.Flags().BoolVar(&noTTY, "no-tty", false, "do not allocate a terminal")
	return cmd
}

// sshCommand is what ssh runs in the container.
// With a terminal, the charm container (an Ubuntu base image) gets the caller's TERM, as kubectl exec sends none, and a login bash when there is no command, as juju does.
// Workload containers can be minimal images, so they get /bin/sh and no env.
func sshCommand(container string, tty bool, term string, command []string) []string {
	if container != charmContainer || !tty {
		if len(command) == 0 {
			return []string{"/bin/sh"}
		}
		return command
	}
	var pre []string
	if term != "" {
		pre = []string{"env", "TERM=" + term}
	}
	if len(command) == 0 {
		command = []string{"bash", "--login"}
	}
	return append(pre, command...)
}

var remotePath = regexp.MustCompile(`^([^/:\s]+/[0-9]+):(.*)$`)

func (c *cli) scpCmd() *cobra.Command {
	var container string
	cmd := &cobra.Command{
		Use:   "scp <source> <destination>",
		Short: "Copy a file between this machine and a unit, as <unit>:<path> (needs tar in the container)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ns, err := c.model()
			if err != nil {
				return err
			}
			remotes := 0
			conv := make([]string, 2)
			for i, a := range args {
				m := remotePath.FindStringSubmatch(a)
				if m == nil {
					conv[i] = a
					continue
				}
				remotes++
				conv[i] = ns + "/" + strings.ReplaceAll(m[1], "/", "-") + ":" + m[2]
			}
			if remotes != 1 {
				return fmt.Errorf("give one unit path, such as app/0:/tmp/file, and one local path")
			}
			return c.kubectl(cmd, "cp", "-c", container, conv[0], conv[1])
		},
	}
	cmd.Flags().StringVar(&container, "container", charmContainer, "container to copy to or from")
	return cmd
}

func unitPod(unit string) (string, error) {
	if !remotePath.MatchString(unit + ":") {
		return "", fmt.Errorf("%q is not a valid unit name", unit)
	}
	return strings.ReplaceAll(unit, "/", "-"), nil
}

func (c *cli) debugLogCmd() *cobra.Command {
	var include []string
	var lines int
	var replay, noTail bool
	cmd := &cobra.Command{
		Use:   "debug-log",
		Short: "Show the logs of the model's unit pods, following them",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, err := c.model()
			if err != nil {
				return err
			}
			cs, err := c.kubeClient()
			if err != nil {
				return err
			}
			pods, err := cs.CoreV1().Pods(ns).List(cmd.Context(), metav1.ListOptions{LabelSelector: v1alpha1.AppLabel})
			if err != nil {
				return err
			}
			names := selectPods(pods.Items, include)
			if len(names) == 0 {
				return fmt.Errorf("no unit pods found")
			}
			opts := &corev1.PodLogOptions{Container: charmContainer, Follow: !noTail}
			if !replay {
				n := int64(lines)
				opts.TailLines = &n
			}
			out := &lockedWriter{w: cmd.OutOrStdout()}
			var wg sync.WaitGroup
			for _, name := range names {
				wg.Add(1)
				go func() {
					defer wg.Done()
					rc, err := cs.CoreV1().Pods(ns).GetLogs(name, opts).Stream(cmd.Context())
					if err != nil {
						printErr(cmd, "%s: %v", name, err)
						return
					}
					defer rc.Close()
					sc := bufio.NewScanner(rc)
					sc.Buffer(make([]byte, 64*1024), 1024*1024)
					for sc.Scan() {
						out.line("unit-" + name + ": " + sc.Text())
					}
				}()
			}
			wg.Wait()
			return nil
		},
	}
	cmd.Flags().StringArrayVar(&include, "include", nil, "show only this unit or application (repeatable)")
	cmd.Flags().IntVar(&lines, "lines", 10, "lines to show before following")
	cmd.Flags().BoolVar(&replay, "replay", false, "show everything the pods logged, not only the last lines")
	cmd.Flags().BoolVar(&noTail, "no-tail", false, "show the logs and stop")
	return cmd
}

// kubeClient is a client-go clientset for reading pod logs.
func (c *cli) kubeClient() (kubernetes.Interface, error) {
	if c.newKube != nil {
		return c.newKube()
	}
	cfg, err := c.flags.ToRESTConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

// selectPods names the unit pods to read, sorted, limited to the units or applications in include.
func selectPods(pods []corev1.Pod, include []string) []string {
	var out []string
	for _, p := range pods {
		app := p.Labels[v1alpha1.AppLabel]
		keep := len(include) == 0
		for _, inc := range include {
			if inc == app || strings.ReplaceAll(inc, "/", "-") == p.Name {
				keep = true
			}
		}
		if keep && strings.HasPrefix(p.Name, app+"-") {
			out = append(out, p.Name)
		}
	}
	sort.Strings(out)
	return out
}

// lockedWriter keeps the lines of concurrent streams whole.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) line(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintln(l.w, s)
}

package hooktools

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
)

// Request is one hook-tool invocation, sent by the multicall client to the agent.
type Request struct {
	ContextID string   `json:"contextId"`
	Name      string   `json:"name"`
	Args      []string `json:"args"`
	Stdin     string   `json:"stdin,omitempty"`
}

// Response carries the tool's output back to the client.
type Response struct {
	Code   int    `json:"code"`
	Stdout string `json:"stdout,omitempty"`
	Stderr string `json:"stderr,omitempty"`
}

// Handler executes hook tools.
type Handler interface {
	Call(req Request) Response
}

// Server serves hook tools on a unix socket for the duration of one hook.
type Server struct {
	l       net.Listener
	h       Handler
	context string
}

// Listen starts serving; the context ID must match JUJU_CONTEXT_ID of the calling hook.
func Listen(network, address, contextID string, h Handler) (*Server, error) {
	if network == "unix" {
		_ = os.Remove(address)
		if err := os.MkdirAll(filepath.Dir(address), 0o755); err != nil {
			return nil, err
		}
	}
	l, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}
	s := &Server{l: l, h: h, context: contextID}
	go s.serve()
	return s, nil
}

// Close stops the server.
func (s *Server) Close() error { return s.l.Close() }

func (s *Server) serve() {
	for {
		c, err := s.l.Accept()
		if err != nil {
			return
		}
		go s.handle(c)
	}
}

func (s *Server) handle(c net.Conn) {
	defer c.Close()
	var req Request
	var resp Response
	if err := json.NewDecoder(c).Decode(&req); err != nil {
		resp = Response{Code: 1, Stderr: "bad request: " + err.Error() + "\n"}
	} else if req.ContextID != s.context {
		resp = Response{Code: 1, Stderr: fmt.Sprintf("wrong context ID %q\n", req.ContextID)}
	} else {
		resp = s.h.Call(req)
	}
	_ = json.NewEncoder(c).Encode(resp)
}

// Main is the multicall client: argv0 is the tool name; it reads the socket from the hook environment.
func Main(name string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	network, address := os.Getenv("JUJU_AGENT_SOCKET_NETWORK"), os.Getenv("JUJU_AGENT_SOCKET_ADDRESS")
	if network == "" || address == "" {
		fmt.Fprintf(stderr, "%s: JUJU_AGENT_SOCKET_ADDRESS/NETWORK not set; not running inside a hook context\n", name)
		return 1
	}
	req := Request{ContextID: os.Getenv("JUJU_CONTEXT_ID"), Name: name, Args: args}
	// Only slurp stdin when the tool is told to read it ("--file -"), so tools never block on an idle terminal.
	for i, a := range args {
		if (a == "--file" && i+1 < len(args) && args[i+1] == "-") || a == "--file=-" {
			b, err := io.ReadAll(stdin)
			if err != nil {
				fmt.Fprintf(stderr, "%s: reading stdin: %v\n", name, err)
				return 1
			}
			req.Stdin = string(b)
		}
	}
	c, err := net.Dial(network, address)
	if err != nil {
		fmt.Fprintf(stderr, "%s: connecting to agent: %v\n", name, err)
		return 1
	}
	defer c.Close()
	if err := json.NewEncoder(c).Encode(req); err != nil {
		fmt.Fprintf(stderr, "%s: %v\n", name, err)
		return 1
	}
	var resp Response
	if err := json.NewDecoder(c).Decode(&resp); err != nil && !errors.Is(err, io.EOF) {
		fmt.Fprintf(stderr, "%s: reading response: %v\n", name, err)
		return 1
	}
	io.WriteString(stdout, resp.Stdout)
	io.WriteString(stderr, resp.Stderr)
	return resp.Code
}

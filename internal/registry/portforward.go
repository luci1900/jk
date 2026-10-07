package registry

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

const (
	systemNamespace = "jk-system"
	serviceName     = "jk-registry"
	authSecret      = "jk-registry-auth"
	servicePort     = 5000
)

// Forward is a running port-forward to jk-registry.
type Forward struct {
	// Endpoint is localhost:<port>.
	Endpoint string
	stop     chan struct{}
}

// Close stops the forward.
func (f *Forward) Close() { close(f.stop) }

// PortForward forwards a local port to a ready pod behind the jk-registry Service.
func PortForward(ctx context.Context, cfg *rest.Config) (*Forward, error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	svc, err := cs.CoreV1().Services(systemNamespace).Get(ctx, serviceName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	pods, err := cs.CoreV1().Pods(systemNamespace).List(ctx, metav1.ListOptions{LabelSelector: labels.SelectorFromSet(svc.Spec.Selector).String()})
	if err != nil {
		return nil, err
	}
	var pod string
	for _, p := range pods.Items {
		if p.Status.Phase == corev1.PodRunning && p.DeletionTimestamp == nil {
			pod = p.Name
			break
		}
	}
	if pod == "" {
		return nil, fmt.Errorf("no running pod behind %s/%s", systemNamespace, serviceName)
	}
	port := int32(servicePort)
	if len(svc.Spec.Ports) > 0 {
		if tp := svc.Spec.Ports[0].TargetPort; tp.IntValue() > 0 {
			port = int32(tp.IntValue())
		}
	}
	rt, upgrader, err := spdy.RoundTripperFor(cfg)
	if err != nil {
		return nil, err
	}
	u := cs.CoreV1().RESTClient().Post().Resource("pods").Namespace(systemNamespace).Name(pod).SubResource("portforward").URL()
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: rt}, http.MethodPost, &url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path})
	stop, ready := make(chan struct{}), make(chan struct{})
	fw, err := portforward.NewOnAddresses(dialer, []string{"127.0.0.1"}, []string{fmt.Sprintf("0:%d", port)}, stop, ready, nil, os.Stderr)
	if err != nil {
		return nil, err
	}
	errc := make(chan error, 1)
	go func() { errc <- fw.ForwardPorts() }()
	select {
	case <-ready:
	case err := <-errc:
		return nil, err
	case <-ctx.Done():
		close(stop)
		return nil, ctx.Err()
	}
	ports, err := fw.GetPorts()
	if err != nil || len(ports) == 0 {
		close(stop)
		return nil, fmt.Errorf("port-forward has no ports: %v", err)
	}
	return &Forward{Endpoint: fmt.Sprintf("127.0.0.1:%d", ports[0].Local), stop: stop}, nil
}

// Connect port-forwards to jk-registry and returns a client with the push credentials from the
// jk-registry-auth Secret, plus a function that closes the forward.
func Connect(ctx context.Context, cfg *rest.Config) (*Client, func(), error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	s, err := cs.CoreV1().Secrets(systemNamespace).Get(ctx, authSecret, metav1.GetOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("reading registry credentials: %w", err)
	}
	f, err := PortForward(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	return &Client{Endpoint: f.Endpoint, Username: string(s.Data["username"]), Password: string(s.Data["password"]), PlainHTTP: true}, f.Close, nil
}

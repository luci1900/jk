package main

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"github.com/luci1900/jk/api/v1alpha1"
)

func newClient(flags *genericclioptions.ConfigFlags) (client.Client, error) {
	cfg, err := flags.ToRESTConfig()
	if err != nil {
		return nil, err
	}
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = apiextensionsv1.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
	return client.New(cfg, client.Options{Scheme: scheme})
}

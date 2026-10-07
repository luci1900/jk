// Command jk-operator is the single cluster-wide jk operator.
package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/operator"
	"github.com/luci1900/jk/internal/registry"
	"github.com/luci1900/jk/internal/version"
	"github.com/luci1900/jk/pkg/charmhub"
)

func main() {
	var leaderElect bool
	flag.BoolVar(&leaderElect, "leader-elect", false, "enable leader election")
	zopts := zap.Options{}
	zopts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zopts)))
	log := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		HealthProbeBindAddress:  ":8081",
		LeaderElection:          leaderElect,
		LeaderElectionID:        "jk-operator.jk.luci1900.github.io",
		LeaderElectionNamespace: v1alpha1.SystemNamespace,
		Cache:                   operator.CacheOptions(),
	})
	if err != nil {
		log.Error(err, "creating manager")
		os.Exit(1)
	}
	_ = mgr.AddHealthzCheck("healthz", healthz.Ping)
	_ = mgr.AddReadyzCheck("readyz", healthz.Ping)

	agentImage := os.Getenv("JK_AGENT_IMAGE")
	if agentImage == "" {
		log.Error(nil, "JK_AGENT_IMAGE is not set (kubectl jk install sets it)")
		os.Exit(1)
	}
	cfg := operator.DefaultConfig(agentImage)
	if v := os.Getenv("JK_REGISTRY_ENDPOINT"); v != "" {
		cfg.RegistryEndpoint = v
	}
	// Reads are anonymous, over plain HTTP in-cluster; pushes use the jk-registry-auth credentials
	// (the Deployment passes them from that Secret).
	charms := &registry.Client{
		Endpoint: cfg.RegistryEndpoint, PlainHTTP: true,
		Username: os.Getenv("JK_REGISTRY_USERNAME"), Password: os.Getenv("JK_REGISTRY_PASSWORD"),
	}
	if charms.Username == "" {
		log.Info("JK_REGISTRY_USERNAME is not set: Charmhub charms cannot be stored")
	}
	store := operator.RegistryStore{Client: charms}
	hub := &charmhub.Client{UserAgent: "jk-operator/" + version.Version}
	if v := os.Getenv("JK_CHARMHUB_URL"); v != "" {
		hub.BaseURL = v
	}
	if err := (&operator.ApplicationReconciler{
		Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Charms: store, Hub: hub, Store: store, Config: cfg,
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "setting up the Application controller")
		os.Exit(1)
	}
	if err := (&operator.ModelReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		log.Error(err, "setting up the model controller")
		os.Exit(1)
	}
	if err := (&operator.RelationReconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader()}).SetupWithManager(mgr); err != nil {
		log.Error(err, "setting up the Relation controller")
		os.Exit(1)
	}
	if err := (&operator.OfferReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		log.Error(err, "setting up the Offer controller")
		os.Exit(1)
	}
	if err := (&operator.ActionReconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader()}).SetupWithManager(mgr); err != nil {
		log.Error(err, "setting up the Action controller")
		os.Exit(1)
	}
	log.Info("starting jk-operator")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "running manager")
		os.Exit(1)
	}
}

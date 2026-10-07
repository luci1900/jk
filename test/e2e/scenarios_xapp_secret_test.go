//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
)

// secretGrants returns the applications named in the grants annotation of the secret's metadata Secret.
func secretGrants(ns, xid string) ([]v1alpha1.SecretGrant, error) {
	var s corev1.Secret
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: v1alpha1.SecretMetadataName(xid)}, &s); err != nil {
		return nil, err
	}
	raw := s.Annotations[v1alpha1.SecretGrantsAnnotation]
	if raw == "" {
		return nil, nil
	}
	var grants []v1alpha1.SecretGrant
	if err := json.Unmarshal([]byte(raw), &grants); err != nil {
		return nil, fmt.Errorf("grants annotation %q: %v", raw, err)
	}
	return grants, nil
}

func grantedTo(grants []v1alpha1.SecretGrant, application string) bool {
	for _, g := range grants {
		if g.Application == application {
			return true
		}
	}
	return false
}

// roleAllows reports whether some Role of the model lets get the named Secret.
func roleAllows(ns, secretName string) (bool, error) {
	var roles rbacv1.RoleList
	if err := kube.List(context.Background(), &roles, client.InNamespace(ns)); err != nil {
		return false, err
	}
	for _, r := range roles.Items {
		for _, rule := range r.Rules {
			for _, n := range rule.ResourceNames {
				if n == secretName {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

// TestCrossAppSecret: the provider's secret is granted to the related application through the relation, the requirer reads it, a new revision reaches it as secret-changed, and revoking the grant stops access.
func TestCrossAppSecret(t *testing.T) {
	t.Parallel()
	ns := newModel(t)
	deploy(t, ns, 1, nil)
	deployClient(t, ns, 1)
	waitUnitsActive(t, 6*time.Minute, ns, app, 1, "clients: 0")
	waitUnitsActive(t, 6*time.Minute, ns, clientApp, 1, "providers: 0")
	rel := relate(t, ns, app, providerEP, clientApp, requirerEP)
	relID := relationID(t, ns, rel.Name)
	waitUnitsActive(t, 3*time.Minute, ns, clientApp, 1, "token: t0")

	var xid string
	t.Run("grant", func(t *testing.T) {
		waitData(t, "provider app data", appRelData(ns, app, relID), map[string]string{"secret-granted": "true"})
		waitNonEmpty(t, "provider secret id", appRelData(ns, app, relID), "secret-id")
		d, _ := peerAppData(ns, app, relID)
		if !strings.HasPrefix(d["secret-id"], "secret:") {
			t.Fatalf("secret id %q", d["secret-id"])
		}
		xid = strings.TrimPrefix(d["secret-id"], "secret:")
		waitData(t, "client read the secret", unitRelData(ns, clientApp, 0, relID), map[string]string{"secret-token": "t0"})
		if d, _ := peerUnitData(ns, clientApp, 0, relID); d["secret-error"] != "" {
			t.Errorf("client has a secret error: %v", d)
		}
		eventually(t, time.Minute, "grant recorded for the client application", func() (bool, error) {
			g, err := secretGrants(ns, xid)
			if err != nil {
				return false, err
			}
			if !grantedTo(g, clientApp) {
				return false, fmt.Errorf("grants %+v", g)
			}
			return true, nil
		})
		eventually(t, time.Minute, "a Role letting the client read revision 1", func() (bool, error) {
			ok, err := roleAllows(ns, v1alpha1.SecretRevisionName(xid, 1))
			if err == nil && !ok {
				err = fmt.Errorf("no Role names %s", v1alpha1.SecretRevisionName(xid, 1))
			}
			return ok, err
		})
	})
	if t.Failed() {
		return
	}

	t.Run("new-revision", func(t *testing.T) {
		setConfigOf(t, ns, app, "bump-data-secret", "1")
		waitUnitsActive(t, 3*time.Minute, ns, clientApp, 1, "token: t1")
		waitNonEmpty(t, "client counted secret-changed", unitRelData(ns, clientApp, 0, relID), "secret-changes")
		waitHooksOf(t, ns, clientApp, 0, "secret-changed")
		if c := count(hookNames(agentLogsOf(t, ns, app, 0)), "secret-changed"); c != 0 {
			t.Errorf("the owner ran secret-changed %d times", c)
		}
		// one immutable Secret per revision
		for rev := 1; rev <= 2; rev++ {
			var s corev1.Secret
			if err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: v1alpha1.SecretRevisionName(xid, rev)}, &s); err != nil {
				t.Errorf("revision %d: %v", rev, err)
			}
		}
		// nobody tracks revision 1 any more, so the owner is told to remove it
		waitHooksOf(t, ns, app, 0, "secret-remove")
	})

	t.Run("revoke", func(t *testing.T) {
		setConfigOf(t, ns, app, "grant-data-secret", "false")
		waitData(t, "provider app data", appRelData(ns, app, relID), map[string]string{"secret-granted": "false"})
		waitData(t, "client saw the revocation", unitRelData(ns, clientApp, 0, relID), map[string]string{"seen-secret-granted": "false"})
		// The client reads the secret on every event; poke it with config changes until a read fails (RBAC can lag a reconcile behind the grant).
		var last time.Time
		poll := 0
		eventually(t, 2*time.Minute, "client cannot read the secret", func() (bool, error) {
			d, err := peerUnitData(ns, clientApp, 0, relID)
			if err != nil {
				return false, err
			}
			if d["secret-error"] != "" {
				return true, nil
			}
			if time.Since(last) > 4*time.Second {
				last = time.Now()
				poll++
				setConfigOf(t, ns, clientApp, "poll", strconv.Itoa(poll))
			}
			return false, fmt.Errorf("client data %v", d)
		})
		eventually(t, time.Minute, "grant removed", func() (bool, error) {
			g, err := secretGrants(ns, xid)
			if err != nil {
				return false, err
			}
			if grantedTo(g, clientApp) {
				return false, fmt.Errorf("grants %+v", g)
			}
			return true, nil
		})
	})
}

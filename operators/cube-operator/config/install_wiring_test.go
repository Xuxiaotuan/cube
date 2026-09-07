package config

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	apps "k8s.io/api/apps/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// Exercise the real installer with a recording kubectl, never the cluster.
func TestAuthorityInstallerWiring(t *testing.T) {
	for _, namespace := range []string{"cube-operator-demo", "authority-install-test"} {
		t.Run(namespace, func(t *testing.T) {
			dir := t.TempDir()
			capture := filepath.Join(dir, "manifests.yaml")
			kubectl := filepath.Join(dir, "kubectl")
			stub := `#!/usr/bin/env bash
set -euo pipefail
[[ "$#" == 3 && "$1" == apply && "$2" == -f ]] || exit 91
if [[ "$3" == - ]]; then cat; else cat "$3"; fi >> "$CAPTURE"
printf '\n---\n' >> "$CAPTURE"
`
			if err := os.WriteFile(kubectl, []byte(stub), 0700); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("bash", "../demo/k8s/run-cubecluster.sh")
			cmd.Env = append(os.Environ(), "KUBECTL="+kubectl, "NAMESPACE="+namespace, "CAPTURE="+capture)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("installer: %v\n%s", err, output)
			}
			data, err := os.ReadFile(capture)
			if err != nil {
				t.Fatal(err)
			}
			roleIndex, bindingIndex, operatorIndex := -1, -1, -1
			finalizers, clusterSeen := false, false
			for index, doc := range strings.Split(string(data), "\n---") {
				var header struct {
					Kind     string `json:"kind"`
					Metadata struct {
						Name string `json:"name"`
					} `json:"metadata"`
				}
				if err := yaml.Unmarshal([]byte(doc), &header); err != nil {
					t.Fatal(err)
				}
				switch header.Kind {
				case "ClusterRole":
					var role rbacv1.ClusterRole
					if err := yaml.Unmarshal([]byte(doc), &role); err != nil {
						t.Fatal(err)
					}
					switch role.Name {
					case "cubestore-authority-tokenreview":
						want := []rbacv1.PolicyRule{{APIGroups: []string{"authentication.k8s.io"}, Resources: []string{"tokenreviews"}, Verbs: []string{"create"}}}
						if !reflect.DeepEqual(role.Rules, want) || role.AggregationRule != nil {
							t.Fatal("TokenReview role broadened")
						}
						roleIndex = index
					case "cubestore-authority-binding-manager":
						want := []rbacv1.PolicyRule{
							{APIGroups: []string{rbacv1.GroupName}, Resources: []string{"clusterroles"}, ResourceNames: []string{"cubestore-authority-tokenreview"}, Verbs: []string{"get", "bind"}},
							{APIGroups: []string{rbacv1.GroupName}, Resources: []string{"clusterrolebindings"}, Verbs: []string{"get", "create", "delete"}},
						}
						if !reflect.DeepEqual(role.Rules, want) || role.AggregationRule != nil {
							t.Fatal("binding permissions broadened")
						}
					default:
						t.Fatalf("unexpected cluster role %s", role.Name)
					}
				case "ClusterRoleBinding":
					var binding rbacv1.ClusterRoleBinding
					if err := yaml.Unmarshal([]byte(doc), &binding); err != nil {
						t.Fatal(err)
					}
					wantRef := rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "cubestore-authority-binding-manager"}
					wantSubjects := []rbacv1.Subject{{Kind: "ServiceAccount", Name: "cube-operator", Namespace: namespace}}
					if binding.Name != "cubestore-authority-binding-manager-"+namespace || binding.RoleRef != wantRef || !reflect.DeepEqual(binding.Subjects, wantSubjects) {
						t.Fatal("wrong installer identity or cluster binding collision")
					}
					// No static TokenReview grant to the Operator or guessed MetaStore
					// SA: the tested controller binds the actual cluster's MetaStore SA.
					bindingIndex = index
				case "Role":
					var role rbacv1.Role
					if err := yaml.Unmarshal([]byte(doc), &role); err != nil {
						t.Fatal(err)
					}
					if role.Name == "cube-operator" && role.Namespace == namespace {
						for _, rule := range role.Rules {
							for _, resource := range rule.Resources {
								if resource == "cubeclusters/finalizers" {
									finalizers = true
								}
							}
						}
					}
				case "Deployment":
					if header.Metadata.Name != "cube-operator" {
						continue
					}
					var deployment apps.Deployment
					if err := yaml.Unmarshal([]byte(doc), &deployment); err != nil {
						t.Fatal(err)
					}
					if deployment.Namespace != namespace || deployment.Spec.Template.Spec.ServiceAccountName != "cube-operator" {
						t.Fatal("operator SA mismatch")
					}
					operatorIndex = index
				case "CubeCluster":
					var c struct {
						Spec map[string]interface{} `json:"spec"`
					}
					if err := yaml.Unmarshal([]byte(doc), &c); err != nil {
						t.Fatal(err)
					}
					if _, enabled := c.Spec["authority"]; enabled {
						t.Fatal("installer unexpectedly enables strict on analytics")
					}
					clusterSeen = true
				}
			}
			if roleIndex < 0 || bindingIndex <= roleIndex || operatorIndex <= bindingIndex || !finalizers || !clusterSeen {
				t.Fatalf("incomplete installer ordering/permissions: role=%d binding=%d operator=%d finalizers=%t cluster=%t", roleIndex, bindingIndex, operatorIndex, finalizers, clusterSeen)
			}
		})
	}
}

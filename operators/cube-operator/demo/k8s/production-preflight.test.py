import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("preflight", Path(__file__).with_name("production-preflight.py"))
preflight = importlib.util.module_from_spec(spec)
spec.loader.exec_module(preflight)


def fixture():
    image = "example.invalid/cube@sha256:" + "a" * 64
    condition = {"conditions": [{"type": "Ready", "status": "True"}]}
    pods = []
    for index, role in enumerate(("api", "refresher", "router", "router", "metastore", "worker")):
        pods.append({"metadata": {"name": role + str(index), "uid": str(index),
                                  "labels": {preflight.LABEL: "demo", preflight.ROLE: role}},
                     "spec": {"nodeName": "n" + str(index % 2),
                              "containers": [{"name": "main", "image": image}]},
                     "status": copy.deepcopy(condition)})
    return {"pods": {"items": pods}, "nodes": {"items": [
        {"metadata": {"name": "n" + str(i)}, "status": copy.deepcopy(condition)} for i in range(2)]},
        "pvcs": {"items": []}, "storageclasses": {"items": []},
        "networkpolicies": {"items": [{"metadata": {"name": "example"}}]},
        "operator": {"spec": {"template": {"spec": {"containers": [{"name": "manager", "image": image}]}}}}}


class PreflightTest(unittest.TestCase):
    def test_static_pass_never_certifies_production(self):
        result = preflight.summarize(fixture(), "demo", 0, 60)
        self.assertEqual(result["status"], "PREFLIGHT_PASS")
        self.assertFalse(result["productionGo"])
        self.assertTrue(result["notVerified"])

    def test_missing_targets_block(self):
        result = preflight.summarize(fixture(), "demo")
        self.assertEqual(len(result["blockers"]), 2)

    def test_same_node_routers_block(self):
        data = fixture()
        for pod in data["pods"]["items"]:
            pod["spec"]["nodeName"] = "n0"
        self.assertIn("routers_not_on_two_ready_nodes", preflight.summarize(data, "demo", 0, 60)["blockers"])

    def test_missing_and_unready_roles_block(self):
        data = fixture()
        data["pods"]["items"].pop()
        data["pods"]["items"][0]["status"] = {}
        result = preflight.summarize(data, "demo", 0, 60)
        self.assertIn("missing_role:worker", result["blockers"])
        self.assertIn("workload_not_ready:api0", result["blockers"])

    def test_tags_and_absent_policy_block(self):
        data = fixture()
        data["pods"]["items"][0]["spec"]["containers"][0]["image"] = "cube:latest"
        data["networkpolicies"]["items"] = []
        result = preflight.summarize(data, "demo", 0, 60)
        self.assertIn("image_not_digest_pinned:api0", result["blockers"])
        self.assertIn("network_policy_absent", result["blockers"])

    def test_missing_then_local_pvc_block(self):
        data = fixture()
        data["pods"]["items"][4]["spec"]["volumes"] = [{"persistentVolumeClaim": {"claimName": "data"}}]
        self.assertIn("missing_pvc:data", preflight.summarize(data, "demo", 0, 60)["blockers"])
        data["pvcs"]["items"] = [{"metadata": {"name": "data"}, "spec": {"storageClassName": "local"},
                                   "status": {"phase": "Bound"}}]
        data["storageclasses"]["items"] = [{"metadata": {"name": "local"}, "provisioner": "rancher.io/local-path"}]
        self.assertIn("node_local_storage_requires_recovery_evidence:data",
                      preflight.summarize(data, "demo", 0, 60)["blockers"])

    def test_unrelated_pods_and_secret_values_not_exported(self):
        data = fixture()
        data["pods"]["items"][0]["spec"]["containers"][0]["env"] = [{"name": "PASSWORD", "value": "do-not-export"}]
        data["pods"]["items"].append({"metadata": {"name": "unrelated"}, "spec": {}})
        self.assertNotIn("do-not-export", str(preflight.summarize(data, "demo", 0, 60)))
        self.assertEqual(len(preflight.summarize(data, "demo", 0, 60)["pods"]), 6)


if __name__ == "__main__":
    unittest.main()

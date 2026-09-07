import importlib.util
import json
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("identity_check", Path(__file__).with_name("authority-tokenreview-check.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class IdentityCheckTest(unittest.TestCase):
    def invoke(self, args, body=None):
        if "lease" in args:
            return json.dumps({"metadata": {"annotations": {"cubejs.io/lease-cluster-id": module.NAMESPACE + "/analytics-router"}},
                               "spec": {"holderIdentity": "router-1"}})
        if "pod" in args:
            return json.dumps({"metadata": {"uid": "pod-uid-1", "labels": {"cubestore.io/component": "router", "cubestore.io/cube-cluster": "analytics"}},
                               "spec": {"serviceAccountName": "analytics-router"}})
        if "token" in args:
            self.assertIn("--bound-object-uid=pod-uid-1", args)
            return "TEST-ONLY-TOKEN-DO-NOT-EXPORT"
        request = json.loads(body)["spec"]
        valid = request["token"] == "TEST-ONLY-TOKEN-DO-NOT-EXPORT" and request["audiences"] == [module.AUDIENCE]
        if not valid:
            return json.dumps({"status": {"authenticated": False}})
        return json.dumps({"status": {"authenticated": True, "audiences": [module.AUDIENCE], "user": {
            "username": "system:serviceaccount:" + module.NAMESPACE + ":analytics-router",
            "extra": {"authentication.kubernetes.io/pod-name": ["router-1"], "authentication.kubernetes.io/pod-uid": ["pod-uid-1"]}}}})

    def test_expected_identity_and_no_token_export(self):
        report = module.check(self.invoke)
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(len(report["results"]), 3)
        self.assertNotIn("TEST-ONLY-TOKEN", json.dumps(report))

    def test_missing_pod_binding_fails(self):
        def invoke(args, body=None):
            result = self.invoke(args, body)
            if body:
                result = json.loads(result)
                result["status"].get("user", {}).pop("extra", None)
                return json.dumps(result)
            return result
        self.assertEqual(module.check(invoke)["status"], "FAIL")

    def test_wrong_audience_acceptance_fails(self):
        def invoke(args, body=None):
            result = self.invoke(args, body)
            if body and json.loads(body)["spec"]["audiences"] == [module.AUDIENCE + "-wrong"]:
                return json.dumps({"status": {"authenticated": True}})
            return result
        self.assertEqual(module.check(invoke)["status"], "FAIL")


if __name__ == "__main__":
    unittest.main()

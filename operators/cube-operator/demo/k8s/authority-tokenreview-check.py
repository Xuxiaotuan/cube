#!/usr/bin/env python3
"""Opt-in, local Kubernetes identity acceptance. Never prints or stores tokens."""

import datetime
import json
import os
import subprocess
import sys

CONTEXT = "orbstack"
NAMESPACE = "cube-ha-remediation"
LEASE = "cube-router-cube-ha-remediation-analytics-router"
AUDIENCE = "cubestore-metastore-authority-v1"


def kubectl(args, body=None):
    result = subprocess.run(
        ["kubectl", "--context", CONTEXT, "--request-timeout=10s"] + args,
        input=body, capture_output=True, text=True, timeout=20)
    if result.returncode:
        # TokenRequest/TokenReview output may contain credentials. Never echo it.
        raise RuntimeError("Kubernetes operation failed; output suppressed")
    return result.stdout


def check(invoke):
    lease = json.loads(invoke(["-n", NAMESPACE, "get", "lease", LEASE, "-o", "json"]))
    if lease["metadata"].get("annotations", {}).get("cubejs.io/lease-cluster-id") != NAMESPACE + "/analytics-router":
        raise ValueError("unexpected Lease identity")
    name = lease["spec"].get("holderIdentity")
    if not name:
        raise ValueError("Lease has no holder")
    pod = json.loads(invoke(["-n", NAMESPACE, "get", "pod", name, "-o", "json"]))
    meta = pod["metadata"]
    labels = meta.get("labels", {})
    if (meta.get("deletionTimestamp") or labels.get("cubestore.io/component") != "router"
            or labels.get("cubestore.io/cube-cluster") != "analytics"):
        raise ValueError("holder is not the expected live Router Pod")
    uid, sa = meta["uid"], pod["spec"]["serviceAccountName"]
    token = invoke(["-n", NAMESPACE, "create", "token", sa, "--audience=" + AUDIENCE,
                    "--duration=10m", "--bound-object-kind=Pod", "--bound-object-name=" + name,
                    "--bound-object-uid=" + uid]).strip()
    if not token:
        raise ValueError("TokenRequest returned no token")
    results = []
    for label, credential, audience, expected in (
            ("bound-audience", token, AUDIENCE, True),
            ("wrong-audience", token, AUDIENCE + "-wrong", False),
            ("invalid-token", "not-a-valid-token", AUDIENCE, False)):
        body = json.dumps({"apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview",
                           "spec": {"token": credential, "audiences": [audience]}})
        status = json.loads(invoke(["create", "--raw", "/apis/authentication.k8s.io/v1/tokenreviews",
                                    "-f", "-"], body))["status"]
        user = status.get("user", {})
        extra = user.get("extra", {})
        authenticated = status.get("authenticated", False)
        passed = authenticated is expected
        if expected:
            passed = (passed and user.get("username") == "system:serviceaccount:" + NAMESPACE + ":" + sa
                      and extra.get("authentication.kubernetes.io/pod-name") == [name]
                      and extra.get("authentication.kubernetes.io/pod-uid") == [uid]
                      and AUDIENCE in status.get("audiences", []))
        results.append({"case": label, "authenticated": authenticated, "pass": bool(passed)})
    return {"status": "PASS" if all(r["pass"] for r in results) else "FAIL",
            "context": CONTEXT, "namespace": NAMESPACE, "podName": name, "podUid": uid,
            "audience": AUDIENCE, "results": results,
            "observedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "scope": "Kubernetes identity only; not CubeStore RPC, MetaStore RBAC, or HA acceptance"}


def main():
    if os.environ.get("CUBE_HA_AUTH_IDENTITY_TEST") != "1":
        print(json.dumps({"status": "NOT_RUN", "reason": "requires CUBE_HA_AUTH_IDENTITY_TEST=1"}))
        return 2
    try:
        report = check(kubectl)
        print(json.dumps(report, indent=2))
        return 0 if report["status"] == "PASS" else 1
    except (subprocess.SubprocessError, OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
        print(json.dumps({"status": "external_blocked", "errorType": type(error).__name__}))
        return 2


if __name__ == "__main__":
    sys.exit(main())

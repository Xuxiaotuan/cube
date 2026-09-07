#!/usr/bin/env python3
"""Read-only deployment prerequisites, not a production certification."""

import argparse
import datetime
import json
import re
import subprocess
import sys


DIGEST = re.compile(r"^.+@sha256:[0-9a-f]{64}$")
LABEL = "cubestore.io/cube-cluster"
ROLE = "cubestore.io/component"


def ready(obj):
    return any(c.get("type") == "Ready" and c.get("status") == "True"
               for c in obj.get("status", {}).get("conditions", []))


def summarize(raw, cluster, rpo=None, rto=None):
    blockers = []
    pods = []
    claims = set()
    for pod in raw["pods"].get("items", []):
        meta, spec = pod["metadata"], pod["spec"]
        if meta.get("labels", {}).get(LABEL) != cluster:
            continue
        if meta.get("deletionTimestamp"):
            blockers.append("workload_terminating:" + meta["name"])
        pod_claims = [v["persistentVolumeClaim"]["claimName"]
                      for v in spec.get("volumes", []) if "persistentVolumeClaim" in v]
        claims.update(pod_claims)
        images = [{"container": c["name"], "image": c["image"],
                   "digestPinned": bool(DIGEST.fullmatch(c["image"]))}
                  for c in spec.get("containers", []) + spec.get("initContainers", [])]
        if not images or any(not i["digestPinned"] for i in images):
            blockers.append("image_not_digest_pinned:" + meta["name"])
        if not ready(pod):
            blockers.append("workload_not_ready:" + meta["name"])
        pods.append({"name": meta["name"], "uid": meta.get("uid"),
                     "role": meta.get("labels", {}).get(ROLE), "node": spec.get("nodeName"),
                     "ready": ready(pod), "images": images, "claims": pod_claims})
    roles = {p["role"] for p in pods}
    for role in ("api", "refresher", "router", "metastore", "worker"):
        if role not in roles:
            blockers.append("missing_role:" + role)
    node_map = {n["metadata"]["name"]: n for n in raw["nodes"].get("items", [])}
    router_nodes = {p["node"] for p in pods if p["role"] == "router" and p["ready"] and p["node"]}
    if len(router_nodes) < 2:
        blockers.append("routers_not_on_two_ready_nodes")
    for name in router_nodes:
        if name not in node_map or not ready(node_map[name]):
            blockers.append("router_node_not_ready:" + name)
    nodes = [{"name": name, "ready": ready(node),
              "zone": node["metadata"].get("labels", {}).get("topology.kubernetes.io/zone")}
             for name, node in sorted(node_map.items())]
    pvc_map = {p["metadata"]["name"]: p for p in raw["pvcs"].get("items", [])}
    sc_map = {s["metadata"]["name"]: s for s in raw["storageclasses"].get("items", [])}
    pvcs = []
    for name in sorted(claims):
        pvc = pvc_map.get(name)
        if pvc is None:
            blockers.append("missing_pvc:" + name)
            continue
        spec, status = pvc.get("spec", {}), pvc.get("status", {})
        sc = spec.get("storageClassName", "")
        provisioner = sc_map.get(sc, {}).get("provisioner")
        if status.get("phase") != "Bound":
            blockers.append("pvc_not_bound:" + name)
        if provisioner is None:
            blockers.append("storage_provisioner_unknown:" + name)
        elif provisioner in ("rancher.io/local-path", "kubernetes.io/no-provisioner"):
            blockers.append("node_local_storage_requires_recovery_evidence:" + name)
        pvcs.append({"name": name, "phase": status.get("phase"), "storageClass": sc,
                     "provisioner": provisioner, "accessModes": spec.get("accessModes", []),
                     "capacity": status.get("capacity", {})})
    policy_names = [p["metadata"]["name"] for p in raw["networkpolicies"].get("items", [])]
    if not policy_names:
        blockers.append("network_policy_absent")
    operator = raw["operator"]
    containers = operator.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    operator_images = [{"container": c["name"], "image": c["image"]} for c in containers]
    if not containers or any(not DIGEST.fullmatch(c["image"]) for c in containers):
        blockers.append("operator_image_not_digest_pinned")
    for field, value in (("rpo_seconds", rpo), ("rto_seconds", rto)):
        if value is None:
            blockers.append("missing_acceptance_target:" + field)
    return {"status": "BLOCKED" if blockers else "PREFLIGHT_PASS", "productionGo": False,
            "cluster": cluster, "targets": {"rpoSeconds": rpo, "rtoSeconds": rto},
            "blockers": blockers, "nodes": nodes, "pods": pods, "pvcs": pvcs,
            "operatorImages": operator_images, "networkPolicyNames": policy_names,
            "notVerified": ["CNI enforcement and effective network policy rules",
                            "independent physical failure domains and node fencing",
                            "volume reattachment, backup restore and object-store consistency",
                            "protocol compatibility and runtime image attestation",
                            "RBAC, drain sequencing and management endpoint authentication",
                            "actual RPO/RTO and complete business recovery"]}


def get_json(context, namespace, resource, name=None):
    cmd = ["kubectl", "--context", context, "--request-timeout=10s"]
    if namespace:
        cmd += ["--namespace", namespace]
    cmd += ["get", resource]
    if name:
        cmd.append(name)
    cmd += ["-o", "json"]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=15)
    return json.loads(result.stdout)


def nonnegative(value):
    result = int(value)
    if result < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--operator", default="cube-operator")
    parser.add_argument("--rpo-seconds", type=nonnegative)
    parser.add_argument("--rto-seconds", type=nonnegative)
    args = parser.parse_args()
    try:
        raw = {"nodes": get_json(args.context, None, "nodes"),
               "storageclasses": get_json(args.context, None, "storageclasses"),
               "pods": get_json(args.context, args.namespace, "pods"),
               "pvcs": get_json(args.context, args.namespace, "pvc"),
               "networkpolicies": get_json(args.context, args.namespace, "networkpolicy"),
               "operator": get_json(args.context, args.namespace, "deployment", args.operator)}
        report = summarize(raw, args.cluster, args.rpo_seconds, args.rto_seconds)
        report.update({"context": args.context, "namespace": args.namespace,
                       "observedAt": datetime.datetime.now(datetime.timezone.utc).isoformat()})
        print(json.dumps(report, indent=2))
        return 0 if report["status"] == "PREFLIGHT_PASS" else 1
    except (subprocess.SubprocessError, OSError, ValueError, KeyError, TypeError) as error:
        print(json.dumps({"status": "external_blocked", "productionGo": False,
                          "errorType": type(error).__name__, "context": args.context,
                          "namespace": args.namespace}))
        return 2


if __name__ == "__main__":
    sys.exit(main())

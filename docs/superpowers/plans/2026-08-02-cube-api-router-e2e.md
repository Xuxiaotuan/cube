# Cube API Router HA E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a real Cube API Pod to the local Kubernetes demo and verify the complete Cube API -> CubeStoreDriver -> leader Service -> Router failover path.

**Architecture:** Build a local Cube API image from the repository's linked workspace packages, configure `CUBEJS_CUBESTORE_HOST` to `cube-router-leader` Service DNS, and expose the API through a demo Service. A real `/cubejs-api/v1/load` request will be issued before and after deleting the active Router leader; Router and operator logs will be captured in the HA report.

**Tech Stack:** Cube.js Node server, modified TypeScript CubeStoreDriver, Docker, Kubernetes Deployment/Service, cube-operator CRD.

## Global Constraints

- Use the existing local `cube-studio-router:ha-local` image and current `cube-operator-demo` namespace.
- Business traffic must use `cube-router-leader` Service, not Router Pod IPs.
- The final report must distinguish direct Router validation from real Cube API end-to-end validation.
- Do not claim write exactly-once semantics without a real `mutationId` and Redis-backed test.

---

### Task 1: Buildable Cube API demo image

**Files:**
- Create: `operators/cube-operator/demo/cube-api/Dockerfile`
- Create: `operators/cube-operator/demo/cube-api/cube.js`
- Create: `operators/cube-operator/demo/cube-api/package.json`

- [ ] **Step 1:** Build the linked `@cubejs-backend/server` package so the local API binary exists.
- [ ] **Step 2:** Create the minimal Cube schema and image entrypoint using the repository workspace packages.
- [ ] **Step 3:** Build `cube-studio-api:ha-local` and confirm the image is available locally.

### Task 2: Kubernetes API deployment

**Files:**
- Create: `operators/cube-operator/demo/k8s/cube-api.yaml`
- Modify: `operators/cube-operator/demo/k8s/run.sh`
- Modify: `operators/cube-operator/demo/k8s/operator-rbac.yaml` only if the deployment needs additional permissions.

- [ ] **Step 1:** Add a Cube API Deployment with `CUBEJS_CUBESTORE_HOST=cube-router-leader.cube-operator-demo.svc.cluster.local` and port `3030`.
- [ ] **Step 2:** Add a Service exposing port `4000`.
- [ ] **Step 3:** Make `run.sh` build/apply the API manifest and wait for rollout.

### Task 3: End-to-end failover runner

**Files:**
- Create: `operators/cube-operator/demo/k8s/cube-api-failover-check.sh`
- Modify: `operators/cube-operator/HA-ROUTER-K8S-DEMO.md`

- [ ] **Step 1:** Query `/cubejs-api/v1/load` through the Cube API Service and record response hashes.
- [ ] **Step 2:** Delete the current Router leader only after the API is ready.
- [ ] **Step 3:** Wait for CR leader, leader Service endpoint, and API response to converge.
- [ ] **Step 4:** Capture Cube API and operator logs and write the real results to the report.

### Task 4: Validation and conclusion

- [ ] **Step 1:** Run the complete build and Kubernetes deployment.
- [ ] **Step 2:** Run the Cube API failover check.
- [ ] **Step 3:** Confirm the result proves the full request path, and document remaining limits.

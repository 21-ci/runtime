---
title: API explorer
description: Interactive reference for every control API operation, rendered with Scalar from the runtime's OpenAPI 3.1 document.
hide:
  - navigation
  - toc
---

# API explorer

Every control API operation, generated from the runtime's OpenAPI 3.1 document. Read the
[API overview](../api.md) first for authentication, errors, pagination and idempotency.
[Download `openapi.json`](../assets/openapi.json){ .md-button }

!!! tip "Sending requests"
    "Test request" targets `http://127.0.0.1:8081` with a Bearer token. Browsers block it unless the control
    API is on the same origin, so for live calls open the same explorer **on your node** at
    `http://127.0.0.1:8081/docs` (no auth needed to view it).

<div id="scalar-api" class="rt-api-explorer" data-spec="../../assets/openapi.json">
  <p class="rt-api-explorer__loading">Loading the API explorer…</p>
</div>

# Traffic statistics branch

`traffic-statistics` is a long-lived, opt-in mbox variant. It is intentionally
separate from `mieru` so machines that do not need persistent traffic accounting
can remain on the existing maintenance and release channel.

## Branch roles

- `mieru` keeps its existing sync and release policy based on `enfein/mbox`.
- `traffic-statistics` is based directly on a reviewed SagerNet/sing-box release,
  with the mbox Mieru protocol patch and the traffic statistics extension applied
  on top.
- Do not merge `traffic-statistics` back into `mieru`, and do not merge `mieru`
  wholesale into `traffic-statistics`. Their upstream baselines can differ.

## Updating the upstream baseline

Keep official sing-box tags in a private ref namespace so fork release tags
cannot collide with them:

```bash
git remote add sing-box git@github.com:SagerNet/sing-box.git
git fetch --no-tags sing-box \
  refs/tags/vX.Y.Z:refs/upstream-tags/sing-box/vX.Y.Z
git fetch origin traffic-statistics
git switch -c sync/traffic-sing-box-vX.Y.Z origin/traffic-statistics
git merge --no-edit refs/upstream-tags/sing-box/vX.Y.Z
```

Resolve conflicts in the Mieru registration, route/group tracing, traffic
tracker, API bridge, and generated option surface as feature conflicts, then run
the focused race tests and the full Go test suite. Push the temporary sync
branch and merge it through a reviewed pull request whose base is
`traffic-statistics`.

Use merge commits for published updates. Do not force-push this long-lived
branch. A push updates only the rolling `traffic-statistics-latest` Linux
release; the normal mbox release channel is unchanged.

## Coordinating with Zashboard

Keep the REST `api_version` stable for additive changes and bump it for
incompatible request or response changes.

Use this synchronization order:

1. Merge the reviewed official sing-box tag into the mbox
   `traffic-statistics` branch, resolve the feature conflicts, and run the Go
   tests.
2. Publish and verify the mbox `traffic-statistics-latest` backend release.
3. Merge current upstream Zashboard `main` into the Zashboard
   `traffic-statistics` branch, retain the capability-gated traffic page, and
   run its type check and production build.
4. Publish the matching Zashboard `traffic-statistics` build.

Do not publish the dashboard first: it may depend on a newer API contract.
Ordinary and older backends remain compatible because Zashboard hides the page
unless the capabilities endpoint reports the supported API version, metric
scope, features, and dimensions.

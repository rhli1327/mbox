# Traffic statistics branch

`traffic-statistics` is a long-lived, opt-in mbox variant. It is intentionally
separate from `mieru` so machines that do not need persistent traffic accounting
can remain on the existing maintenance and release channel.

## Branch roles

- `mieru` is the maintained mbox base branch and the long-term Git upstream of
  `traffic-statistics`. Official sing-box baseline updates, Mieru updates, and
  fixes shared by both variants land on `mieru` first.
- `traffic-statistics` contains the opt-in traffic statistics extension on top
  of `mieru`.
- Do not merge `traffic-statistics` back into `mieru`, and do not implement
  shared changes directly on `traffic-statistics`. Keep the dependency
  direction `mieru` -> `traffic-statistics`.

## Updating the upstream baseline

First update and review `mieru`. Keep official sing-box tags in a private ref
namespace so fork release tags cannot collide with them:

```bash
git remote add sing-box git@github.com:SagerNet/sing-box.git
git fetch --no-tags sing-box \
  refs/tags/vX.Y.Z:refs/upstream-tags/sing-box/vX.Y.Z
git fetch origin mieru
git switch -c sync/mieru-sing-box-vX.Y.Z origin/mieru
git merge --no-edit refs/upstream-tags/sing-box/vX.Y.Z
```

Resolve Mieru registration and generated option conflicts as feature conflicts,
then run the relevant tests. Push the temporary sync branch and merge it through
a reviewed pull request whose base is `mieru`.

Only after the updated `mieru` branch and its normal release are verified,
merge it into the statistics branch:

```bash
git fetch origin mieru traffic-statistics
git switch traffic-statistics
git merge --no-edit origin/mieru
```

Resolve traffic tracker, route/group tracing, API bridge, documentation, and
workflow conflicts without dropping either the shared Mieru behavior or the
statistics extension. Run the focused race tests and the broader Go test suite
before publishing.

Use merge commits for published updates. Do not force-push this long-lived
branch. A push updates only the rolling `traffic-statistics-latest` Linux
release; the normal mbox release channel is unchanged.

## Coordinating with Zashboard

Keep the REST `api_version` stable for additive changes and bump it for
incompatible request or response changes.

Use this synchronization order:

1. Update and verify the mbox `mieru` branch, including any shared build or
   release changes.
2. Merge current `origin/mieru` into mbox `traffic-statistics`, resolve the
   statistics feature conflicts, and run the Go tests.
3. Publish and verify the mbox `traffic-statistics-latest` backend release.
4. Merge current upstream Zashboard `main` into the Zashboard
   `traffic-statistics` branch, retain the capability-gated traffic page, and
   run its type check and production build.
5. Publish the matching Zashboard `traffic-statistics` build.

Do not publish the dashboard first: it may depend on a newer API contract.
Ordinary and older backends remain compatible because Zashboard hides the page
unless the capabilities endpoint reports the supported API version, metric
scope, features, and dimensions.

package build_shared

import (
	"fmt"
	"strings"

	"github.com/sagernet/sing-box/common/badversion"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/shell"

	"golang.org/x/mod/semver"
)

func ReadTag() (string, error) {
	currentTag, err := shell.Exec("git", "describe", "--tags").ReadOutput()
	if err != nil {
		return currentTag, err
	}
	currentTagRev, _ := shell.Exec("git", "describe", "--tags", "--abbrev=0").ReadOutput()
	if currentTagRev == currentTag {
		return currentTag[1:], nil
	}
	shortCommit, _ := shell.Exec("git", "rev-parse", "--short", "HEAD").ReadOutput()
	version := badversion.Parse(currentTagRev[1:])
	return version.String() + "-" + shortCommit, nil
}

func ReadTagVersionRev() (badversion.Version, error) {
	currentTagRev := common.Must1(shell.Exec("git", "describe", "--tags", "--abbrev=0").ReadOutput())
	return badversion.Parse(currentTagRev[1:]), nil
}

func ReadTagVersion() (badversion.Version, error) {
	currentTag := common.Must1(shell.Exec("git", "describe", "--tags").ReadOutput())
	currentTagRev := common.Must1(shell.Exec("git", "describe", "--tags", "--abbrev=0").ReadOutput())
	version := badversion.Parse(currentTagRev[1:])
	if currentTagRev != currentTag {
		if version.PreReleaseIdentifier == "" {
			version.Patch++
		}
	}
	return version, nil
}

func ReadHighestReachableVersion(revision string) (string, error) {
	tagsOutput, err := shell.Exec("git", "tag", "--merged", revision, "--list", "v[0-9]*").ReadOutput()
	if err != nil {
		return "", err
	}
	highestTag := highestVersionTag(strings.Fields(tagsOutput))
	if highestTag == "" {
		return "", fmt.Errorf("no semantic version tag is reachable from %s", revision)
	}
	return strings.TrimPrefix(highestTag, "v"), nil
}

func highestVersionTag(tags []string) string {
	var highestTag string
	for _, tag := range tags {
		if !semver.IsValid(tag) || isMboxBuildTag(tag) {
			continue
		}
		if highestTag == "" || semver.Compare(tag, highestTag) > 0 {
			highestTag = tag
		}
	}
	return highestTag
}

func isMboxBuildTag(tag string) bool {
	return strings.Contains(tag, "+mbox.") || strings.Contains(tag, "-mbox.")
}

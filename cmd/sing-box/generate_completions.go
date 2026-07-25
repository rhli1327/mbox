//go:build generate && generate_completions

package main

import "github.com/sagernet/sing-box/log"

func main() {
	err := generateCompletions()
	if err != nil {
		log.Fatal(err)
	}
}

func generateCompletions() error {
	err := mainCommand.GenBashCompletionFile("release/completions/mbox.bash")
	if err != nil {
		return err
	}
	err = mainCommand.GenFishCompletionFile("release/completions/mbox.fish", true)
	if err != nil {
		return err
	}
	err = mainCommand.GenZshCompletionFile("release/completions/mbox.zsh")
	if err != nil {
		return err
	}
	return nil
}

package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

func main() {
	edge := flag.String("edge-config", "", "Caddy configuration path")
	api := flag.String("openapi", "docs/openapi.yaml", "OpenAPI path")
	flag.Parse()
	if err := check(*edge, *api); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func check(edge, api string) error {
	config, err := os.ReadFile(edge)
	if err != nil {
		return fmt.Errorf("read edge config: %w", err)
	}
	raw, err := os.ReadFile(api)
	if err != nil {
		return err
	}
	var spec struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		return err
	}
	var matcher *regexp.Regexp
	for _, line := range strings.Split(string(config), "\n") {
		fields := strings.Fields(strings.SplitN(line, "#", 2)[0])
		if len(fields) < 3 || fields[0] != "@stream" || fields[1] != "path_regexp" {
			continue
		}
		pattern := fields[len(fields)-1]
		matcher, err = regexp.Compile(strings.Trim(pattern, "\"`"))
		if err != nil {
			return fmt.Errorf("invalid @stream path_regexp: %w", err)
		}
		break
	}
	if matcher == nil {
		return fmt.Errorf("edge config has no @stream path_regexp matcher")
	}
	parameter := regexp.MustCompile(`\{[^}]+\}`)
	count := 0
	for path, methods := range spec.Paths {
		sample := parameter.ReplaceAllString(path, "00000000-0000-4000-8000-000000000001")
		if methods["x-served-by"] != "stream-gateway" {
			if matcher.MatchString(sample) {
				return fmt.Errorf("@stream incorrectly captures control route %s", path)
			}
			continue
		}
		if !matcher.MatchString(sample) {
			return fmt.Errorf("@stream does not cover gateway route %s", path)
		}
		count++
	}
	if count == 0 {
		return fmt.Errorf("OpenAPI has no stream-gateway routes")
	}
	fmt.Printf("edge @stream covers %d gateway routes\n", count)
	return nil
}

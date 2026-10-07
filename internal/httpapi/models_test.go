package httpapi

import (
	"reflect"
	"testing"
)

func TestModelCatalogFiltersAndKeepsOrder(t *testing.T) {
	items, fallback := modelCatalog(
		[]string{"glm-5", "not a model", "glm-5-flash", "glm-5", "", "../etc/passwd"},
		"glm-5",
	)
	if !reflect.DeepEqual(items, []string{"glm-5", "glm-5-flash"}) {
		t.Errorf("items: %q", items)
	}
	if fallback != "glm-5" {
		t.Errorf("fallback: %q", fallback)
	}
}

func TestModelCatalogDropsAMalformedDefault(t *testing.T) {
	items, fallback := modelCatalog([]string{"glm-5"}, "not a model")
	if !reflect.DeepEqual(items, []string{"glm-5"}) {
		t.Errorf("items: %q", items)
	}
	if fallback != "" {
		t.Errorf("fallback: %q", fallback)
	}
}

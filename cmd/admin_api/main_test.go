package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestRenderListAndShow(t *testing.T) {
	updated := time.Date(2026, 11, 10, 21, 35, 29, 0, time.UTC)
	var list bytes.Buffer
	renderList(&list, []jobSummary{{
		ID:           "chatcmpl_proxy_0123456789abcdef0123456789abcdef",
		State:        "delivered",
		Model:        "deepseek/deepseek-v4-flash",
		Path:         "/chat/completions",
		UpdatedAt:    updated,
		ResultStatus: "completed",
	}})
	if !strings.Contains(list.String(), "chatcmpl_proxy_0123456789abcdef0123456789abcdef") || !strings.Contains(list.String(), "completed") || !strings.Contains(list.String(), "2026-11-10 21:35:29") {
		t.Fatalf("list table:\n%s", list.String())
	}

	var show bytes.Buffer
	renderShow(&show, storedJob{
		ID:            "chatcmpl_proxy_0123456789abcdef0123456789abcdef",
		Path:          "/chat/completions",
		Model:         "deepseek/deepseek-v4-flash",
		State:         "delivered",
		WebhookURL:    "https://example.com/hook",
		WebhookSecret: "whsec_example",
		Attempts:      1,
		Result:        []byte(`{"status":"completed","choices":[{"message":{"content":"Hello from the model."}}]}`),
	})
	got := show.String()
	if !strings.Contains(got, "whsec_example") || !strings.Contains(got, "Hello from the model.") || !strings.Contains(got, "output") {
		t.Fatalf("show table:\n%s", got)
	}
}

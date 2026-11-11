package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
)

func main() {
	loadDotEnv()
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	token := os.Getenv("ADMIN_TOKEN")
	host := strings.TrimRight(os.Getenv("ADMIN_API_HOST"), "/")
	if token == "" || host == "" {
		fmt.Fprintln(stderr, "Usage: bin/admin_api ls")
		fmt.Fprintln(stderr, "       bin/admin_api show JOB_ID")
		return fmt.Errorf("put ADMIN_TOKEN and ADMIN_API_HOST in .env")
	}
	switch {
	case len(args) == 1 && args[0] == "ls":
		body, err := getJSON(host+"/admin/jobs", token)
		if err != nil {
			return err
		}
		var list struct {
			Jobs []jobSummary `json:"jobs"`
		}
		if err = json.Unmarshal(body, &list); err != nil {
			return err
		}
		renderList(stdout, list.Jobs)
	case len(args) == 2 && args[0] == "show" && args[1] != "":
		body, err := getJSON(host+"/admin/jobs/"+args[1], token)
		if err != nil {
			return err
		}
		var job storedJob
		if err = json.Unmarshal(body, &job); err != nil {
			return err
		}
		renderShow(stdout, job)
	default:
		fmt.Fprintln(stderr, "Usage: bin/admin_api ls")
		fmt.Fprintln(stderr, "       bin/admin_api show JOB_ID")
		return fmt.Errorf("put ADMIN_TOKEN and ADMIN_API_HOST in .env")
	}
	return nil
}

type jobSummary struct {
	ID           string    `json:"id"`
	State        string    `json:"state"`
	Model        string    `json:"model"`
	Path         string    `json:"path"`
	UpdatedAt    time.Time `json:"updated_at"`
	ResultStatus string    `json:"result_status"`
}

type storedJob struct {
	ID            string          `json:"id"`
	Path          string          `json:"path"`
	Model         string          `json:"model"`
	State         string          `json:"state"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
	WebhookURL    string          `json:"webhook_url"`
	WebhookSecret string          `json:"webhook_secret"`
	Attempts      int             `json:"webhook_attempts"`
	DeliveryError string          `json:"delivery_error"`
	Request       json.RawMessage `json:"request"`
	Result        json.RawMessage `json:"result"`
}

func renderList(w io.Writer, jobs []jobSummary) {
	tw := newTable(w)
	tw.AppendHeader(table.Row{"ID", "State", "Result", "Model", "Updated"})
	for _, job := range jobs {
		tw.AppendRow(table.Row{job.ID, job.State, job.ResultStatus, job.Model, formatTime(job.UpdatedAt)})
	}
	tw.Render()
}

func renderShow(w io.Writer, job storedJob) {
	var status string
	if len(job.Result) > 0 {
		var body struct {
			Status string `json:"status"`
		}
		_ = json.Unmarshal(job.Result, &body)
		status = body.Status
	}
	tw := newTable(w)
	tw.AppendHeader(table.Row{"Field", "Value"})
	rows := []table.Row{
		{"id", job.ID},
		{"path", job.Path},
		{"model", job.Model},
		{"state", job.State},
		{"result", status},
		{"created", formatTime(job.CreatedAt)},
		{"updated", formatTime(job.UpdatedAt)},
		{"webhook url", job.WebhookURL},
		{"webhook secret", job.WebhookSecret},
		{"attempts", job.Attempts},
	}
	if job.DeliveryError != "" {
		rows = append(rows, table.Row{"delivery error", job.DeliveryError})
	}
	tw.AppendRows(rows)
	tw.SetColumnConfigs([]table.ColumnConfig{{
		Number:           2,
		WidthMax:         88,
		WidthMaxEnforcer: text.WrapSoft,
	}})
	tw.Render()
	if len(bytes.TrimSpace(job.Request)) > 0 {
		fmt.Fprintln(w)
		renderText(w, "request", string(job.Request))
	}
	fmt.Fprintln(w)
	renderText(w, "output", outputText(job))
}

func renderText(w io.Writer, title, body string) {
	tw := newTable(w)
	tw.SetTitle(title)
	tw.AppendRow(table.Row{body})
	tw.SetColumnConfigs([]table.ColumnConfig{{
		Number:           1,
		WidthMax:         88,
		WidthMaxEnforcer: text.WrapSoft,
	}})
	tw.Render()
}

func outputText(job storedJob) string {
	if len(bytes.TrimSpace(job.Result)) == 0 {
		return "(no model output yet)"
	}
	var body struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Output []struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(job.Result, &body) != nil {
		return string(job.Result)
	}
	var parts []string
	for _, choice := range body.Choices {
		if choice.Message.Content != "" {
			parts = append(parts, choice.Message.Content)
		}
	}
	for _, item := range body.Output {
		for _, content := range item.Content {
			if content.Text != "" {
				parts = append(parts, content.Text)
			}
		}
	}
	if len(parts) > 0 {
		return strings.Join(parts, "\n")
	}
	if body.Error.Message != "" {
		return body.Error.Message
	}
	return string(job.Result)
}

func newTable(w io.Writer) table.Writer {
	tw := table.NewWriter()
	tw.SetOutputMirror(w)
	tw.SetStyle(table.StyleRounded)
	return tw
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02 15:04:05")
}

func getJSON(url, token string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	client := &http.Client{Timeout: 60 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s returned HTTP %d: %s", url, res.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func loadDotEnv() {
	root := repoRoot()
	if root == "" {
		return
	}
	raw, err := os.ReadFile(filepath.Join(root, ".env"))
	if err != nil {
		return
	}
	for line := range strings.SplitSeq(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if len(value) >= 2 {
			if value[0] == '"' && value[len(value)-1] == '"' || value[0] == '\'' && value[len(value)-1] == '\'' {
				value = value[1 : len(value)-1]
			}
		}
		_ = os.Setenv(key, value)
	}
}

func repoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

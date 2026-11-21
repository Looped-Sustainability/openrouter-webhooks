package main

import (
	"encoding/json"
	"time"
)

func (j *job) response() json.RawMessage {
	if len(j.Result) > 0 {
		return j.Result
	}
	status := "queued"
	if j.State == "running" {
		status = "in_progress"
	}
	var body map[string]any
	switch j.Path {
	case "/responses":
		body = map[string]any{"id": j.ID, "object": "response", "created_at": j.CreatedAt.Unix(), "status": status, "background": true, "model": j.Model, "output": []any{}, "completed_at": nil, "frequency_penalty": nil, "presence_penalty": nil, "instructions": nil, "error": nil, "incomplete_details": nil, "metadata": map[string]any{}, "parallel_tool_calls": true, "tools": []any{}, "tool_choice": "auto", "temperature": 1, "top_p": 1, "text": map[string]any{"format": map[string]any{"type": "text"}}, "truncation": "disabled"}
	case "/messages":
		body = map[string]any{"id": j.ID, "type": "message", "role": "assistant", "model": j.Model, "content": []any{}, "stop_reason": nil, "stop_sequence": nil, "usage": map[string]any{"input_tokens": 0, "output_tokens": 0}, "status": status, "background": true}
	case "/systemone":
		body = map[string]any{"id": j.ID, "object": "systemone", "model": j.Model, "status": status, "background": true, "answers": map[string]any{}}
	default:
		body = map[string]any{"id": j.ID, "object": "chat.completion", "created": j.CreatedAt.Unix(), "model": j.Model, "choices": []any{}, "system_fingerprint": nil, "status": status, "background": true}
	}
	data, _ := json.Marshal(body)
	return data
}
func (j *job) finish(raw []byte, statusCode int, status, message string) {
	var body map[string]any
	_ = json.Unmarshal(j.response(), &body)
	var upstream map[string]any
	if json.Unmarshal(raw, &upstream) == nil {
		for k, v := range upstream {
			body[k] = v
		}
	}
	if upstreamID, ok := body["id"].(string); ok && upstreamID != j.ID {
		body["upstream_id"] = upstreamID
	}
	body["id"] = j.ID
	body["status"] = status
	body["background"] = true
	if message != "" && body["error"] == nil {
		body["error"] = map[string]any{"code": status, "message": message}
	}
	switch j.Path {
	case "/responses":
		body["object"] = "response"
		if body["output"] == nil {
			body["output"] = []any{}
		}
	case "/messages":
		body["type"] = "message"
		if body["content"] == nil {
			body["content"] = []any{}
		}
	case "/systemone":
		body["object"] = "systemone"
		if body["answers"] == nil {
			body["answers"] = map[string]any{}
		}
	default:
		body["object"] = "chat.completion"
		if body["choices"] == nil {
			body["choices"] = []any{}
		}
	}
	j.Result, _ = json.Marshal(body)
	j.Event = &event{ID: "evt_" + newID(), Object: "event", Type: eventPrefix(j.Path) + status, CreatedAt: time.Now().Unix(), Data: j.Result, StatusCode: statusCode}
	j.State = "delivering"
	j.Credential = ""
	j.Request = nil
	j.Headers = nil
	j.UpdatedAt = time.Now().UTC()
}

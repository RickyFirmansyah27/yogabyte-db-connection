package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
)

var Logger *slog.Logger

type ContextHandler struct {
	opts slog.HandlerOptions
	out  io.Writer
	mu   sync.Mutex
}

func NewContextHandler(out io.Writer, opts slog.HandlerOptions) *ContextHandler {
	return &ContextHandler{
		opts: opts,
		out:  out,
	}
}

func (h *ContextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.opts.Level.Level()
}

func (h *ContextHandler) Handle(ctx context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Format time: Mon, 02 Jan 2006 15:04:05
	timeStr := r.Time.Format("Mon, 02 Jan 2006 15:04:05")

	// Get Level String
	level := r.Level.String()

	// Helper to extract attributes
	var component string
	var attrs map[string]any = make(map[string]any)

	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "component" {
			component = a.Value.String()
		} else {
			attrs[a.Key] = a.Value.Any()
		}
		return true
	})

	msg := r.Message

	// If it's the specific format with "Request | ..." or "Response | ...",
	// we might want to just print it as is if that matches the user desire,
	// but the user wanted: timestamp [LEVEL]: message

	// Format: timestamp [LEVEL]: message
	// The component logic handles previous requirement, but user just updated requirement to Winston sample.
	// Winston sample: "ddd, DD MMM YYYY HH:mm:ss [LEVEL]: message"

	// We will keep component logic just in case, or append it to message if present and not part of message structure

	finalMsg := msg
	if component != "" {
		// keeping old behavior slightly just in case but fitting new format
		// actually user didn't show component in the LAST sample, but it was there in previous.
		// Last sample:
		// Request | Method: GET | Headers: ... | URL: /
		// Response | Method: GET | URL: / | Status: 200 | Duration: ...

		// Winston format wrapper:
		// timestamp [LEVEL]: message

		// So we just print:
		// time [LEVEL]: Request | ...

		// If component exists, maybe prepend to message?
		// The user didn't explicitly ask to remove component support, but the sample logic was purely based on message
		// I will ignore component special formatting for now to match the strict Winston sample provided.
	}

	fmt.Fprintf(h.out, "%s [%s]: %s", timeStr, level, finalMsg)

	// Print other attributes if any
	if len(attrs) > 0 {
		var attrsStrs []string
		for k, v := range attrs {
			attrsStrs = append(attrsStrs, fmt.Sprintf("%s=%v", k, v))
		}
		if len(attrsStrs) > 0 {
			fmt.Fprintf(h.out, " | %s", strings.Join(attrsStrs, " "))
		}
	}
	fmt.Fprintln(h.out)

	return nil
}

func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return h
}

func InitLogger() {
	handler := NewContextHandler(os.Stdout, slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	Logger = slog.New(handler)

	slog.SetDefault(Logger)
}

func GetLogger() *slog.Logger {
	if Logger == nil {
		InitLogger()
	}
	return Logger
}

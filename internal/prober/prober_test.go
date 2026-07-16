package prober

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sentinelhq-sh/outpost/internal/reporter"
)

func TestProbeHTTP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			w.WriteHeader(200)
			_, _ = w.Write([]byte("service healthy"))
		case "/boom":
			w.WriteHeader(500)
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()

	cases := []struct {
		name   string
		spec   reporter.ProbeSpec
		wantUp bool
	}{
		{
			name:   "200 expected and matches",
			spec:   reporter.ProbeSpec{Kind: "http", Target: srv.URL + "/ok", ExpectedStatus: 200, TimeoutMs: 2000},
			wantUp: true,
		},
		{
			name:   "500 fails expected 200",
			spec:   reporter.ProbeSpec{Kind: "http", Target: srv.URL + "/boom", ExpectedStatus: 200, TimeoutMs: 2000},
			wantUp: false,
		},
		{
			name:   "keyword present",
			spec:   reporter.ProbeSpec{Kind: "http", Target: srv.URL + "/ok", ExpectedStatus: 200, Keyword: "healthy", TimeoutMs: 2000},
			wantUp: true,
		},
		{
			name:   "keyword absent",
			spec:   reporter.ProbeSpec{Kind: "http", Target: srv.URL + "/ok", ExpectedStatus: 200, Keyword: "degraded", TimeoutMs: 2000},
			wantUp: false,
		},
		{
			name:   "no expected status accepts any 2xx",
			spec:   reporter.ProbeSpec{Kind: "http", Target: srv.URL + "/ok", TimeoutMs: 2000},
			wantUp: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := Probe(context.Background(), tc.spec)
			if res.Up != tc.wantUp {
				t.Fatalf("Up = %v, want %v (msg: %q, status: %d)", res.Up, tc.wantUp, res.Message, res.StatusCode)
			}
		})
	}
}

func TestProbeHTTPUserAgent(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("User-Agent")
		w.WriteHeader(200)
	}))
	defer srv.Close()

	Probe(context.Background(), reporter.ProbeSpec{Kind: "http", Target: srv.URL, TimeoutMs: 2000})

	if want := probeUserAgent(); got != want {
		t.Fatalf("User-Agent = %q, want %q", got, want)
	}
}

func TestProbeTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	up := Probe(context.Background(), reporter.ProbeSpec{Kind: "tcp", Target: ln.Addr().String(), TimeoutMs: 1000})
	if !up.Up {
		t.Fatalf("expected open port to be up, got msg %q", up.Message)
	}

	down := Probe(context.Background(), reporter.ProbeSpec{Kind: "tcp", Target: "127.0.0.1:1", TimeoutMs: 500})
	if down.Up {
		t.Fatal("expected closed port to be down")
	}
}

func TestProbeDNS(t *testing.T) {
	res := Probe(context.Background(), reporter.ProbeSpec{Kind: "dns", Target: "localhost", TimeoutMs: 2000})
	if !res.Up {
		t.Fatalf("expected localhost to resolve, got msg %q", res.Message)
	}

	bad := Probe(context.Background(), reporter.ProbeSpec{Kind: "dns", Target: "nonexistent.invalid.", TimeoutMs: 2000})
	if bad.Up {
		t.Fatal("expected invalid host to fail resolution")
	}
}

func TestProbeUnknownKind(t *testing.T) {
	res := Probe(context.Background(), reporter.ProbeSpec{Kind: "smtp", Target: "x", TimeoutMs: 1000})
	if res.Up {
		t.Fatal("unknown kind must be down")
	}
}

//go:build e2e

package e2e

import (
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"testing"
)

// The ports of the proxies are below the ephemeral range of Linux (32768) and macOS (49152), where
// outgoing connections and Docker take theirs, and each one is a proxy's for the life of the process:
// a proxy that refuses gives its port up, and must get the same one back when it resumes.
const (
	proxyPortMin = 20000
	proxyPortMax = 32767
)

var proxyPorts = struct {
	sync.Mutex
	taken map[int]bool
}{taken: map[int]bool{}}

// listenProxyPort listens on a port of its own in the proxy range.
func listenProxyPort() (net.Listener, error) {
	proxyPorts.Lock()
	defer proxyPorts.Unlock()
	for range 100 {
		port := proxyPortMin + rand.IntN(proxyPortMax-proxyPortMin+1)
		if proxyPorts.taken[port] {
			continue
		}
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		proxyPorts.taken[port] = true
		return ln, nil
	}
	return nil, fmt.Errorf("no free port in %d-%d", proxyPortMin, proxyPortMax)
}

// proxy is a TCP forwarder that can fail on demand. binwave talks to MySQL and ClickHouse through
// one, so a scenario can cut a database off in the ways a network does: refusing connections,
// resetting the open ones, or swallowing everything without ever answering (a frozen peer).
type proxy struct {
	t      *testing.T
	target string
	addr   string

	mu        sync.Mutex
	ln        net.Listener
	conns     map[net.Conn]struct{}
	blackhole bool
	closed    bool
}

func newProxy(t *testing.T, target string) *proxy {
	t.Helper()
	p := &proxy{t: t, target: target, conns: map[net.Conn]struct{}{}}
	ln, err := listenProxyPort()
	if err != nil {
		t.Fatal(err)
	}
	p.ln, p.addr = ln, ln.Addr().String()
	go p.accept(ln)
	t.Cleanup(p.close)
	return p
}

func (p *proxy) accept(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go p.serve(c)
	}
}

func (p *proxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.conns[c] = struct{}{}
	return true
}

func (p *proxy) serve(client net.Conn) {
	if !p.track(client) {
		_ = client.Close()
		return
	}
	p.mu.Lock()
	hole := p.blackhole
	p.mu.Unlock()
	if hole {
		// Accept and never answer: the client waits on a peer that is gone.
		_, _ = io.Copy(io.Discard, client)
		return
	}

	p.mu.Lock()
	target := p.target
	p.mu.Unlock()
	server, err := net.Dial("tcp", target)
	if err != nil {
		_ = client.Close()
		return
	}
	if !p.track(server) {
		_ = client.Close()
		_ = server.Close()
		return
	}
	go p.pipe(server, client)
	p.pipe(client, server)
}

// pipe copies from src to dst until either side ends; a blackhole swallows the data instead.
func (p *proxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32*1024)
	defer func() { _ = dst.Close(); _ = src.Close() }()
	for {
		n, err := src.Read(buf)
		if n > 0 {
			p.mu.Lock()
			hole := p.blackhole
			p.mu.Unlock()
			if !hole {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *proxy) dropConns() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
}

// Refuse stops listening and resets every open connection: the server looks down.
func (p *proxy) Refuse() {
	p.mu.Lock()
	ln := p.ln
	p.ln = nil
	p.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	p.dropConns()
}

// Retarget points the proxy at another backend address and resets the connections to the old one. It
// is what keeps binwave's address fixed across a restart of the server, which Docker gives a new port.
func (p *proxy) Retarget(target string) {
	p.mu.Lock()
	p.target = target
	p.mu.Unlock()
	p.dropConns()
}

// Blackhole keeps connections open but answers nothing, like a frozen server or a partition.
func (p *proxy) Blackhole() {
	p.mu.Lock()
	p.blackhole = true
	p.mu.Unlock()
}

// Resume restores normal forwarding. Connections that were swallowed or reset stay dead.
func (p *proxy) Resume() {
	p.mu.Lock()
	p.blackhole = false
	needListener := p.ln == nil
	p.mu.Unlock()
	p.dropConns()
	if needListener {
		ln, err := net.Listen("tcp", p.addr)
		if err != nil {
			p.t.Fatalf("resume proxy on %s: %v", p.addr, err)
		}
		p.mu.Lock()
		p.ln = ln
		p.mu.Unlock()
		go p.accept(ln)
	}
}

func (p *proxy) close() {
	p.mu.Lock()
	p.closed = true
	ln := p.ln
	p.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	p.dropConns()
}

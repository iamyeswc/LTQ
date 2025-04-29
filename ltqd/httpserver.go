package ltqd

import (
	"errors"
	"fmt"
	"net"
	"net/http"
)

func Serve(listener net.Listener, handler http.Handler, proto string) error {
	fmtLogf(Debug, "%s: listening on %s", proto, listener.Addr())

	server := &http.Server{
		Handler: handler,
	}
	err := server.Serve(listener)
	// theres no direct way to detect this error because it is not exposed
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("http.Serve() error - %s", err)
	}

	fmtLogf(Debug, "%s: closing %s", proto, listener.Addr())

	return nil
}

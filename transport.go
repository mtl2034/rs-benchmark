/*
# rs-benchmark - A utility to benchmark object storages
# Copyright (C) 2016-2019 RStor Inc (open-source@rstor.io)
#
# This file is part of rs-benchmark.
#
# rs-benchmark is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# rs-benchmark is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Copyright Header.  If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

var httpClient = &http.Client{
	Transport: HTTPTransport,
	Timeout:   time.Minute * 5,
}

var dialer = &net.Dialer{
	Timeout:   30 * time.Second,
	KeepAlive: 30 * time.Second,
}

// Our HTTP transport used for the roundtripper below
var HTTPTransport http.RoundTripper = &http.Transport{
	Proxy: http.ProxyFromEnvironment,

	DialContext: dialer.DialContext,

	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 0,

	ResponseHeaderTimeout: 10 * time.Second,

	// Allow an unlimited number of idle connections
	MaxIdleConnsPerHost: 4096,
	MaxIdleConns:        0,

	// But limit their idle time
	IdleConnTimeout: time.Minute,

	// Ignore TLS errors
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}

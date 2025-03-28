// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Skip tests on Windows temporarily, see https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/11451
//go:build !windows
// +build !windows

package components

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/ballastextension"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/extension/zpagesextension"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/asapauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/basicauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/bearertokenauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/headerssetterextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/httpforwarder"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/jaegerremotesampling"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/oauth2clientauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer/ecstaskobserver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer/hostobserver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/pprofextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/sigv4authextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/dbstorage"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/testutil"
)

func TestDefaultExtensions(t *testing.T) {
	allFactories, err := Components()
	require.NoError(t, err)

	extFactories := allFactories.Extensions
	endpoint := testutil.GetAvailableLocalAddress(t)

	tests := []struct {
		extension     component.Type
		getConfigFn   getExtensionConfigFn
		skipLifecycle bool
	}{
		{
			extension: "health_check",
			getConfigFn: func() component.Config {
				cfg := extFactories["health_check"].CreateDefaultConfig().(*healthcheckextension.Config)
				cfg.Endpoint = endpoint
				return cfg
			},
		},
		{
			extension: "pprof",
			getConfigFn: func() component.Config {
				cfg := extFactories["pprof"].CreateDefaultConfig().(*pprofextension.Config)
				cfg.TCPAddr.Endpoint = endpoint
				return cfg
			},
		},
		{
			extension: "sigv4auth",
			getConfigFn: func() component.Config {
				cfg := extFactories["sigv4auth"].CreateDefaultConfig().(*sigv4authextension.Config)
				return cfg
			},
		},
		{
			extension: "zpages",
			getConfigFn: func() component.Config {
				cfg := extFactories["zpages"].CreateDefaultConfig().(*zpagesextension.Config)
				cfg.TCPAddr.Endpoint = endpoint
				return cfg
			},
		},
		{
			extension: "basicauth",
			getConfigFn: func() component.Config {
				cfg := extFactories["basicauth"].CreateDefaultConfig().(*basicauthextension.Config)
				// No need to clean up, t.TempDir will be deleted entirely.
				fileName := filepath.Join(t.TempDir(), "random.file")
				require.NoError(t, os.WriteFile(fileName, []byte("username:password"), 0600))

				cfg.Htpasswd = &basicauthextension.HtpasswdSettings{
					File:   fileName,
					Inline: "username:password",
				}
				return cfg
			},
		},
		{
			extension: "bearertokenauth",
			getConfigFn: func() component.Config {
				cfg := extFactories["bearertokenauth"].CreateDefaultConfig().(*bearertokenauthextension.Config)
				cfg.BearerToken = "sometoken"
				return cfg
			},
		},
		{
			extension: "memory_ballast",
			getConfigFn: func() component.Config {
				cfg := extFactories["memory_ballast"].CreateDefaultConfig().(*ballastextension.Config)
				return cfg
			},
		},
		{
			extension: "asapclient",
			getConfigFn: func() component.Config {
				cfg := extFactories["asapclient"].CreateDefaultConfig().(*asapauthextension.Config)
				cfg.KeyID = "test_issuer/test_kid"
				cfg.Issuer = "test_issuer"
				cfg.Audience = []string{"some_service"}
				cfg.TTL = 10 * time.Second
				// Valid PEM data required for successful initialisation. Key not actually used anywhere.
				cfg.PrivateKey = "data:application/pkcs8;kid=test;base64,MIIBUwIBADANBgkqhkiG9w0BAQEFAASCAT0wggE5AgE" +
					"AAkEA0ZPr5JeyVDoB8RyZqQsx6qUD+9gMFg1/0hgdAvmytWBMXQJYdwkK2dFJwwZcWJVhJGcOJBDfB/8tcbdJd34KZQIDAQ" +
					"ABAkBZD20tJTHJDSWKGsdJyNIbjqhUu4jXTkFFPK4Hd6jz3gV3fFvGnaolsD5Bt50dTXAiSCpFNSb9M9GY6XUAAdlBAiEA6" +
					"MccfdZRfVapxKtAZbjXuAgMvnPtTvkVmwvhWLT5Wy0CIQDmfE8Et/pou0Jl6eM0eniT8/8oRzBWgy9ejDGfj86PGQIgWePq" +
					"IL4OofRBgu0O5TlINI0HPtTNo12U9lbUIslgMdECICXT2RQpLcvqj+cyD7wZLZj6vrHZnTFVrnyR/cL2UyxhAiBswe/MCcD" +
					"7T7J4QkNrCG+ceQGypc7LsxlIxQuKh5GWYA=="
				return cfg
			},
		},
		{
			extension: "ecs_task_observer",
			getConfigFn: func() component.Config {
				cfg := extFactories["ecs_task_observer"].CreateDefaultConfig().(*ecstaskobserver.Config)
				cfg.Endpoint = "http://localhost"
				return cfg
			},
		},
		{
			extension:     "awsproxy",
			skipLifecycle: true, // Requires EC2 metadata service to be running
		},
		{
			extension: "http_forwarder",
			getConfigFn: func() component.Config {
				cfg := extFactories["http_forwarder"].CreateDefaultConfig().(*httpforwarder.Config)
				cfg.Egress.Endpoint = "http://" + endpoint
				cfg.Ingress.Endpoint = testutil.GetAvailableLocalAddress(t)
				return cfg
			},
		},
		{
			extension: "oauth2client",
			getConfigFn: func() component.Config {
				cfg := extFactories["oauth2client"].CreateDefaultConfig().(*oauth2clientauthextension.Config)
				cfg.ClientID = "otel-extension"
				cfg.ClientSecret = "testsarehard"
				cfg.TokenURL = "http://" + endpoint
				return cfg
			},
		},
		{
			extension:     "oidc",
			skipLifecycle: true, // Requires a running OIDC server in order to complete life cycle testing
		},
		{
			extension: "db_storage",
			getConfigFn: func() component.Config {
				cfg := extFactories["db_storage"].CreateDefaultConfig().(*dbstorage.Config)
				cfg.DriverName = "sqlite3"
				cfg.DataSource = filepath.Join(t.TempDir(), "foo.db")
				return cfg
			},
		},
		{
			extension: "file_storage",
			getConfigFn: func() component.Config {
				cfg := extFactories["file_storage"].CreateDefaultConfig().(*filestorage.Config)
				cfg.Directory = t.TempDir()
				return cfg
			},
		},
		{
			extension: "host_observer",
			getConfigFn: func() component.Config {
				cfg := extFactories["host_observer"].CreateDefaultConfig().(*hostobserver.Config)
				return cfg
			},
		},
		{
			extension:     "k8s_observer",
			skipLifecycle: true, // Requires a K8s api to interfact with and validate
		},
		{
			extension: "headers_setter",
			getConfigFn: func() component.Config {
				cfg := extFactories["headers_setter"].CreateDefaultConfig().(*headerssetterextension.Config)
				return cfg
			},
		},
		{
			extension:     "jaegerremotesampling",
			skipLifecycle: true,
			getConfigFn: func() component.Config {
				return extFactories["jaegerremotesampling"].CreateDefaultConfig().(*jaegerremotesampling.Config)
			},
		},
	}

	extensionCount := 0
	expectedExtensions := map[component.Type]struct{}{}
	for k := range extFactories {
		expectedExtensions[k] = struct{}{}
	}
	for _, tt := range tests {
		_, ok := extFactories[tt.extension]
		if !ok {
			// not part of the distro, skipping.
			continue
		}
		delete(expectedExtensions, tt.extension)
		extensionCount++
		t.Run(string(tt.extension), func(t *testing.T) {
			factory := extFactories[tt.extension]
			assert.Equal(t, tt.extension, factory.Type())

			verifyExtensionShutdown(t, factory, tt.getConfigFn)

			if !tt.skipLifecycle {
				verifyExtensionLifecycle(t, factory, tt.getConfigFn)
			}

		})
	}
	assert.Len(t, extFactories, extensionCount, "All extensions must be added to the lifecycle tests", expectedExtensions)
}

// getExtensionConfigFn is used customize the configuration passed to the verification.
// This is used to change ports or provide values required but not provided by the
// default configuration.
type getExtensionConfigFn func() component.Config

// verifyExtensionLifecycle is used to test if an extension type can handle the typical
// lifecycle of a component. The getConfigFn parameter only need to be specified if
// the test can't be done with the default configuration for the component.
func verifyExtensionLifecycle(t *testing.T, factory extension.Factory, getConfigFn getExtensionConfigFn) {
	ctx := context.Background()
	host := newAssertNoErrorHost(t)
	extCreateSet := extensiontest.NewNopCreateSettings()

	if getConfigFn == nil {
		getConfigFn = factory.CreateDefaultConfig
	}

	firstExt, err := factory.CreateExtension(ctx, extCreateSet, getConfigFn())
	require.NoError(t, err)
	require.NoError(t, firstExt.Start(ctx, host))
	require.NoError(t, firstExt.Shutdown(ctx))

	secondExt, err := factory.CreateExtension(ctx, extCreateSet, getConfigFn())
	require.NoError(t, err)
	require.NoError(t, secondExt.Start(ctx, host))
	require.NoError(t, secondExt.Shutdown(ctx))
}

// verifyExtensionShutdown is used to test if an extension type can be shutdown without being started first.
func verifyExtensionShutdown(tb testing.TB, factory extension.Factory, getConfigFn getExtensionConfigFn) {
	ctx := context.Background()
	extCreateSet := extensiontest.NewNopCreateSettings()

	if getConfigFn == nil {
		getConfigFn = factory.CreateDefaultConfig
	}

	e, err := factory.CreateExtension(ctx, extCreateSet, getConfigFn())
	if errors.Is(err, component.ErrDataTypeIsNotSupported) {
		return
	}
	if e == nil {
		return
	}

	assert.NotPanics(tb, func() {
		assert.NoError(tb, e.Shutdown(ctx))
	})
}

// assertNoErrorHost implements a component.Host that asserts that there were no errors.
type assertNoErrorHost struct {
	component.Host
	*testing.T
}

var _ component.Host = (*assertNoErrorHost)(nil)

// newAssertNoErrorHost returns a new instance of assertNoErrorHost.
func newAssertNoErrorHost(t *testing.T) component.Host {
	return &assertNoErrorHost{
		componenttest.NewNopHost(),
		t,
	}
}

func (aneh *assertNoErrorHost) ReportFatalError(err error) {
	assert.NoError(aneh, err)
}

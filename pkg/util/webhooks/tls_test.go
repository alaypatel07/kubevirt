package webhooks_test

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	k8sv1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/certificate"

	"kubevirt.io/kubevirt/pkg/certificates/triple/cert"
	"kubevirt.io/kubevirt/pkg/util/webhooks"
	"kubevirt.io/kubevirt/pkg/virt-operator/resource/generate/components"
)

type mockCAManager struct {
	caBundle []byte
}

type mockCertManager struct {
	crt *tls.Certificate
}

func (m *mockCertManager) Start() {
	panic("implement me")
}

func (m *mockCertManager) Stop() {
	panic("implement me")
}

func (m *mockCertManager) Current() *tls.Certificate {
	return m.crt
}

func (m *mockCertManager) ServerHealthy() bool {
	panic("implement me")
}

func (m *mockCAManager) GetCurrent() (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	certs, _ := cert.ParseCertsPEM(m.caBundle)
	for _, crt := range certs {
		pool.AddCert(crt)
	}
	return pool, nil
}

var _ = Describe("TLS", func() {

	var caManager webhooks.ClientCAManager
	var certmanagers map[string]certificate.Manager

	BeforeEach(func() {
		// Bootstrap TLS for kubevirt
		certmanagers = map[string]certificate.Manager{}
		caSecrets := components.NewCACertSecrets("whatever")
		var caSecret *k8sv1.Secret
		for _, ca := range caSecrets {
			if ca.Name == components.KubeVirtCASecretName {
				caSecret = ca
			}
		}

		secrets := components.NewCertSecrets("install_namespace", "operator_namespace")
		Expect(components.PopulateSecretWithCertificate(caSecret, nil, &v1.Duration{Duration: 1 * time.Hour})).To(Succeed())
		caCert, err := components.LoadCertificates(caSecret)
		Expect(err).ToNot(HaveOccurred())
		for _, secret := range secrets {
			Expect(components.PopulateSecretWithCertificate(secret, caCert, &v1.Duration{Duration: 1 * time.Hour})).To(Succeed())
			crt, err := components.LoadCertificates(secret)
			certmanagers[secret.Name] = &mockCertManager{crt: crt}
			Expect(err).ToNot(HaveOccurred())
		}
		caBundle := cert.EncodeCertPEM(caCert.Leaf)
		caManager = &mockCAManager{caBundle: caBundle}
	})

	DescribeTable("on virt-handler with self-signed CA should", func(serverSecret, clientSecret string, errStr string) {
		serverTLSConfig := webhooks.SetupTLSForVirtHandlerServer(caManager, certmanagers[serverSecret], false)
		clientTLSConfig := webhooks.SetupTLSForVirtHandlerClients(caManager, certmanagers[clientSecret], false)
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "hello")
		}))
		srv.TLS = serverTLSConfig
		srv.StartTLS()
		defer srv.Close()
		srv.Client()
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: clientTLSConfig}}
		resp, err := client.Get(srv.URL)
		if errStr == "" {
			Expect(err).ToNot(HaveOccurred())
		} else {
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring(errStr))
			return
		}
		body, err := ioutil.ReadAll(resp.Body)
		Expect(err).ToNot(HaveOccurred())
		Expect(strings.TrimSpace(string(body))).To(Equal("hello"))
	},
		Entry(
			"connect with proper certificates",
			components.VirtHandlerServerCertSecretName,
			components.VirtHandlerCertSecretName,
			"",
		),
		Entry(
			"fail if client uses not a client certificate",
			components.VirtHandlerServerCertSecretName,
			components.VirtHandlerServerCertSecretName,
			"remote error: tls: bad certificate",
		),
		Entry(
			"fail if server uses not a server certificate",
			components.VirtHandlerCertSecretName,
			components.VirtHandlerCertSecretName,
			"x509: certificate specifies an incompatible key usage",
		),
	)

	DescribeTable("on virt-handler with externally-managed certificates should", func(serverSecret, clientSecret string, errStr string) {
		serverTLSConfig := webhooks.SetupTLSForVirtHandlerServer(caManager, certmanagers[serverSecret], true)
		clientTLSConfig := webhooks.SetupTLSForVirtHandlerClients(caManager, certmanagers[clientSecret], true)
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "hello")
		}))
		srv.TLS = serverTLSConfig
		srv.StartTLS()
		defer srv.Close()
		srv.Client()
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: clientTLSConfig}}
		resp, err := client.Get(srv.URL)
		if errStr == "" {
			Expect(err).ToNot(HaveOccurred())
		} else {
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring(errStr))
			return
		}
		body, err := ioutil.ReadAll(resp.Body)
		Expect(err).ToNot(HaveOccurred())
		Expect(strings.TrimSpace(string(body))).To(Equal("hello"))
	},
		Entry(
			"connect with proper certificates",
			components.VirtHandlerServerCertSecretName,
			components.VirtHandlerCertSecretName,
			"",
		),
		Entry(
			"fail if client uses not a client certificate",
			components.VirtHandlerServerCertSecretName,
			components.VirtHandlerServerCertSecretName,
			"remote error: tls: bad certificate",
		),
		Entry(
			"fail if server uses not a server certificate",
			components.VirtHandlerCertSecretName,
			components.VirtHandlerCertSecretName,
			"x509: certificate specifies an incompatible key usage",
		),
	)

	It("should allow anonymous TLS connections to prometheus endpoints", func() {
		serverTLSConfig := webhooks.SetupPromTLS(certmanagers[components.VirtHandlerServerCertSecretName])
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "hello")
		}))
		srv.TLS = serverTLSConfig
		srv.StartTLS()
		defer srv.Close()
		srv.Client()
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
		resp, err := client.Get(srv.URL)
		Expect(err).ToNot(HaveOccurred())
		body, err := ioutil.ReadAll(resp.Body)
		Expect(err).ToNot(HaveOccurred())
		Expect(strings.TrimSpace(string(body))).To(Equal("hello"))
	})

	DescribeTable("should verify self-signed client and server certificates", func(serverSecret, clientSecret string, errStr string) {
		serverTLSConfig := webhooks.SetupTLSWithCertManager(caManager, certmanagers[serverSecret], tls.RequireAndVerifyClientCert)
		clientTLSConfig := webhooks.SetupTLSForVirtHandlerClients(caManager, certmanagers[clientSecret], false)
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "hello")
		}))
		srv.TLS = serverTLSConfig
		srv.StartTLS()
		defer srv.Close()
		srv.Client()
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: clientTLSConfig}}
		resp, err := client.Get(srv.URL)
		if errStr == "" {
			Expect(err).ToNot(HaveOccurred())
		} else {
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring(errStr))
			return
		}
		body, err := ioutil.ReadAll(resp.Body)
		Expect(err).ToNot(HaveOccurred())
		Expect(strings.TrimSpace(string(body))).To(Equal("hello"))
	},
		Entry(
			"connect with proper certificates",
			components.VirtHandlerServerCertSecretName,
			components.VirtHandlerCertSecretName,
			"",
		),
		Entry(
			"fail if client uses an invalid certificate",
			components.VirtHandlerServerCertSecretName,
			components.VirtHandlerServerCertSecretName,
			"remote error: tls: bad certificate",
		),
		Entry(
			"fail if server uses an invalid certificate",
			components.VirtHandlerCertSecretName,
			components.VirtHandlerCertSecretName,
			"x509: certificate specifies an incompatible key usage",
		),
	)

	DescribeTable("should verify externally-managed client and server certificates", func(serverSecret, clientSecret string, errStr string) {
		serverTLSConfig := webhooks.SetupTLSWithCertManager(caManager, certmanagers[serverSecret], tls.RequireAndVerifyClientCert)
		clientTLSConfig := webhooks.SetupTLSForVirtHandlerClients(caManager, certmanagers[clientSecret], true)
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "hello")
		}))
		srv.TLS = serverTLSConfig
		srv.StartTLS()
		defer srv.Close()
		srv.Client()
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: clientTLSConfig}}
		resp, err := client.Get(srv.URL)
		if errStr == "" {
			Expect(err).ToNot(HaveOccurred())
		} else {
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring(errStr))
			return
		}
		body, err := ioutil.ReadAll(resp.Body)
		Expect(err).ToNot(HaveOccurred())
		Expect(strings.TrimSpace(string(body))).To(Equal("hello"))
	},
		Entry(
			"connect with proper certificates",
			components.VirtHandlerServerCertSecretName,
			components.VirtHandlerCertSecretName,
			"",
		),
		Entry(
			"fail if client uses an invalid certificate",
			components.VirtHandlerServerCertSecretName,
			components.VirtHandlerServerCertSecretName,
			"remote error: tls: bad certificate",
		),
		Entry(
			"fail if server uses an invalid certificate",
			components.VirtHandlerCertSecretName,
			components.VirtHandlerCertSecretName,
			"x509: certificate specifies an incompatible key usage",
		),
	)
})

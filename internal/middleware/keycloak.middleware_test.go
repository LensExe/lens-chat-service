package middleware

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"go-app/pkg/setting"
	"net/http/httptest"
	"testing"
	"time"
)

func TestKeycloakVerification(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	cfg := setting.KeycloakConfig{Issuer: "https://identity.example/realms/app", Audience: "chat-service", PublicKey: string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))}
	verifier, err := NewKeycloakVerifier(cfg)
	if err != nil {
		t.Fatal(err)
	}
	base := func() KeycloakClaims {
		return KeycloakClaims{TokenType: "Bearer", RegisteredClaims: jwt.RegisteredClaims{Issuer: cfg.Issuer, Audience: jwt.ClaimStrings{cfg.Audience}, Subject: "keycloak-user-uuid", ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}}
	}
	cases := []struct {
		name   string
		change func(*KeycloakClaims)
		valid  bool
	}{
		{"valid", func(c *KeycloakClaims) {}, true},
		{"wrong issuer", func(c *KeycloakClaims) { c.Issuer = "other" }, false},
		{"wrong audience", func(c *KeycloakClaims) { c.Audience = jwt.ClaimStrings{"other"} }, false},
		{"missing audience", func(c *KeycloakClaims) { c.Audience = nil }, false},
		{"expired", func(c *KeycloakClaims) { c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Minute)) }, false},
		{"missing expiry", func(c *KeycloakClaims) { c.ExpiresAt = nil }, false},
		{"future nbf", func(c *KeycloakClaims) { c.NotBefore = jwt.NewNumericDate(time.Now().Add(time.Hour)) }, false},
		{"missing subject", func(c *KeycloakClaims) { c.Subject = "" }, false},
		{"id token", func(c *KeycloakClaims) { c.TokenType = "ID" }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claims := base()
			tc.change(&claims)
			raw, e := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(key)
			if e != nil {
				t.Fatal(e)
			}
			_, e = verifier.Verify(raw)
			if (e == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, e)
			}
		})
	}
	for _, raw := range []string{"", "malformed", "a.b.c"} {
		if _, err = verifier.Verify(raw); err == nil {
			t.Fatal("accepted malformed token")
		}
	}
	raw, err := jwt.NewWithClaims(jwt.SigningMethodHS256, base()).SignedString([]byte(cfg.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = verifier.Verify(raw); err == nil {
		t.Fatal("accepted algorithm confusion")
	}
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = jwt.NewWithClaims(jwt.SigningMethodRS256, base()).SignedString(other)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = verifier.Verify(raw); err == nil {
		t.Fatal("accepted forged signature")
	}
	raw, err = jwt.NewWithClaims(jwt.SigningMethodRS256, base()).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/", verifier.GinMiddleware(), func(c *gin.Context) {
		if UserID(c) != "keycloak-user-uuid" || TenantID(c) != "default" {
			t.Error("wrong principal")
		}
		c.Status(204)
	})
	for _, tc := range []struct {
		header, query string
		status        int
	}{{"Bearer " + raw, "", 204}, {"", "?token=" + raw, 401}, {"Bearer invalid", "", 401}} {
		req := httptest.NewRequest("GET", "/"+tc.query, nil)
		req.Header.Set("Authorization", tc.header)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code != tc.status {
			t.Fatalf("status=%d want=%d", rec.Code, tc.status)
		}
	}
}

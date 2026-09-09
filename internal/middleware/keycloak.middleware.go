package middleware

import (
	"crypto/rsa"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"go-app/pkg/response"
	"go-app/pkg/setting"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

const (
	ContextUserID    = "auth.user_id"
	ContextTenantID  = "auth.tenant_id"
	ContextExpiresAt = "auth.expires_at"
)

type KeycloakClaims struct {
	TokenType         string `json:"typ"`
	TenantID          string `json:"tenant_id,omitempty"`
	PreferredUsername string `json:"preferred_username,omitempty"`
	jwt.RegisteredClaims
}

type KeycloakVerifier struct {
	publicKey *rsa.PublicKey
	issuer    string
	audience  string
}

func NewKeycloakVerifier(cfg setting.KeycloakConfig) (*KeycloakVerifier, error) {
	if cfg.Issuer == "" || cfg.Audience == "" {
		return nil, errors.New("Keycloak issuer and audience are required")
	}
	keyData := []byte(strings.ReplaceAll(cfg.PublicKey, `\n`, "\n"))
	if len(keyData) == 0 && cfg.PublicKeyFile != "" {
		var err error
		keyData, err = os.ReadFile(cfg.PublicKeyFile)
		if err != nil {
			return nil, fmt.Errorf("read Keycloak public key: %w", err)
		}
	}
	if len(keyData) == 0 {
		return nil, errors.New("Keycloak public key is empty")
	}
	if !strings.Contains(string(keyData), "BEGIN") {
		keyData = []byte("-----BEGIN PUBLIC KEY-----\n" + wrapBase64(string(keyData)) + "\n-----END PUBLIC KEY-----")
	}
	publicKey, err := jwt.ParseRSAPublicKeyFromPEM(keyData)
	if err != nil {
		return nil, fmt.Errorf("parse Keycloak RSA public key: %w", err)
	}
	return &KeycloakVerifier{publicKey: publicKey, issuer: cfg.Issuer, audience: cfg.Audience}, nil
}

func wrapBase64(value string) string {
	value = strings.Join(strings.Fields(value), "")
	var lines []string
	for len(value) > 64 {
		lines = append(lines, value[:64])
		value = value[64:]
	}
	if value != "" {
		lines = append(lines, value)
	}
	return strings.Join(lines, "\n")
}

func (v *KeycloakVerifier) Verify(rawToken string) (*KeycloakClaims, error) {
	options := []jwt.ParserOption{
		jwt.WithValidMethods([]string{jwt.SigningMethodRS256.Alg()}),
		jwt.WithIssuer(v.issuer),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(30 * time.Second),
	}
	if v.audience != "" {
		options = append(options, jwt.WithAudience(v.audience))
	}

	claims := &KeycloakClaims{}
	token, err := jwt.ParseWithClaims(rawToken, claims, func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != jwt.SigningMethodRS256.Alg() {
			return nil, fmt.Errorf("unexpected signing method %q", token.Method.Alg())
		}
		return v.publicKey, nil
	}, options...)
	if err != nil || token == nil || !token.Valid || strings.TrimSpace(claims.Subject) == "" || claims.TokenType != "Bearer" {
		return nil, errors.New("invalid Keycloak access token")
	}
	return claims, nil
}

func (v *KeycloakVerifier) GinMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		rawToken := bearerToken(c.GetHeader("Authorization"))
		if rawToken == "" && c.GetHeader("Authorization") == "" && strings.EqualFold(c.GetHeader("Upgrade"), "websocket") {
			rawToken = websocketSubprotocolToken(c.GetHeader("Sec-WebSocket-Protocol"))
		}
		claims, err := v.Verify(rawToken)
		if err != nil {
			response.Unauthorized(c)
			c.Abort()
			return
		}
		tenantID := claims.TenantID
		if tenantID == "" {
			tenantID = "default"
		}
		c.Set(ContextUserID, claims.Subject)
		c.Set(ContextTenantID, tenantID)
		c.Set(ContextExpiresAt, claims.ExpiresAt.Time)
		c.Next()
	}
}

func bearerToken(header string) string {
	parts := strings.Fields(header)
	if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
		return parts[1]
	}
	return ""
}

// Browser clients can connect with: new WebSocket(url, ["access_token", token]).
// The access token is verified locally and is never sent to Keycloak per request.
func websocketSubprotocolToken(header string) string {
	parts := strings.Split(header, ",")
	for index := range parts {
		parts[index] = strings.TrimSpace(parts[index])
	}
	for index := 0; index+1 < len(parts); index++ {
		if parts[index] == "access_token" {
			return parts[index+1]
		}
	}
	return ""
}

func UserID(c *gin.Context) string {
	return c.GetString(ContextUserID)
}

func TenantID(c *gin.Context) string {
	return c.GetString(ContextTenantID)
}

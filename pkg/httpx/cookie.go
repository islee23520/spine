package httpx

import (
	"encoding/base64"
	"time"
)

type Cookie struct {
	Name  string
	Value string

	Path    string
	Domain  string
	MaxAge  int
	Expires *time.Time

	HttpOnly bool
	Secure   bool
	SameSite SameSite

	Priority string // 허용값: "Low", "Medium", "High"
}

type SameSite string

const (
	SameSiteLax    SameSite = "Lax"
	SameSiteStrict SameSite = "Strict"
	SameSiteNone   SameSite = "None"
)

// EncodeCookieValue는 임의의 UTF-8 텍스트나 바이너리 문자열 데이터를
// Cookie.Value에 안전하게 사용할 수 있는 패딩 없는 base64url 값으로 변환합니다.
func EncodeCookieValue(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

// DecodeCookieValue는 EncodeCookieValue의 변환을 되돌립니다. 잘못된 base64url 입력은
// 일부만 디코딩한 데이터를 반환하지 않고 오류로 처리합니다.
func DecodeCookieValue(value string) (string, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}

const (
	AccessTokenCookieName  = "accessToken"
	RefreshTokenCookieName = "refreshToken"
)

func AccessTokenCookie(token string, ttl time.Duration) Cookie {
	return Cookie{
		Name:     AccessTokenCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,

		Secure:   true,
		SameSite: SameSiteNone,

		MaxAge: int(ttl.Seconds()),
	}
}

func RefreshTokenCookie(token string, ttl time.Duration) Cookie {
	return Cookie{
		Name:     RefreshTokenCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,

		Secure:   true,
		SameSite: SameSiteNone,

		MaxAge: int(ttl.Seconds()),
	}
}

func DefaultRefreshTokenCookie(token string) Cookie {
	return RefreshTokenCookie(token, 7*24*time.Hour)
}

func ClearAccessTokenCookie() Cookie {
	return Cookie{
		Name:   AccessTokenCookieName,
		Value:  "",
		Path:   "/",
		MaxAge: -1,
	}
}

func ClearRefreshTokenCookie() Cookie {
	return Cookie{
		Name:   RefreshTokenCookieName,
		Value:  "",
		Path:   "/",
		MaxAge: -1,
	}
}

import jwt
import secrets
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from fastapi import HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import hashlib
import hmac
import base64


class TokenPayload(BaseModel):
    """JWT token payload structure."""

    sub: str  # username
    exp: datetime
    scopes: list = []
    iss: str = "voice-feature-store"


class User(BaseModel):
    """User model for authentication."""

    username: str
    hashed_password: str
    scopes: list = ["read:features", "write:features"]
    disabled: bool = False


class SecurityConfig:
    """Security configuration manager."""

    def __init__(self):
        self.secret_key = self._get_secret_key()
        self.algorithm = "HS256"
        self.access_token_expire_minutes = 30

    def _get_secret_key(self) -> str:
        """Get secret key from environment or generate a secure one."""
        import os

        key = os.getenv("JWT_SECRET_KEY")
        if not key:
            # In production, this should always be set via environment
            key = secrets.token_urlsafe(32)
        return key

    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash."""
        try:
            algorithm, salt, hash_value = hashed_password.split("$")
            key = hashlib.pbkdf2_hmac(
                algorithm,
                plain_password.encode("utf-8"),
                salt.encode("ascii"),
                100000,  # Number of iterations
            )
            return hmac.compare_digest(
                hash_value, base64.b64encode(key).decode("ascii")
            )
        except Exception:
            return False

    def get_password_hash(self, password: str) -> str:
        """Hash a password for storage."""
        salt = secrets.token_hex(16)
        key = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), salt.encode("ascii"), 100000
        )
        hash_value = base64.b64encode(key).decode("ascii")
        return f"sha256${salt}${hash_value}"


class Authenticator:
    """Handle authentication and authorization."""

    def __init__(self, security_config: SecurityConfig):
        self.config = security_config
        self.users_db = self._initialize_users()
        self.security = HTTPBearer()

    def _initialize_users(self) -> Dict[str, User]:
        """Initialize user database (in production, use a real database)."""
        # These would typically come from a database
        return {
            "api_user": User(
                username="api_user",
                hashed_password=self.config.get_password_hash("secure_password_123"),
                scopes=["read:features", "write:features", "read:metrics"],
            ),
            "ml_engineer": User(
                username="ml_engineer",
                hashed_password=self.config.get_password_hash("ml_password_456"),
                scopes=[
                    "read:features",
                    "write:features",
                    "read:training_data",
                    "read:metrics",
                ],
            ),
            "admin": User(
                username="admin",
                hashed_password=self.config.get_password_hash("admin_password_789"),
                scopes=[
                    "read:features",
                    "write:features",
                    "read:training_data",
                    "read:metrics",
                    "admin",
                ],
            ),
        }

    def authenticate_user(self, username: str, password: str) -> Optional[User]:
        """Authenticate a user with username and password."""
        user = self.users_db.get(username)
        if not user or user.disabled:
            return None
        if not self.config.verify_password(password, user.hashed_password):
            return None
        return user

    def create_access_token(self, user: User) -> str:
        """Create a JWT access token."""
        expires_delta = timedelta(minutes=self.config.access_token_expire_minutes)
        expire = datetime.utcnow() + expires_delta

        payload = TokenPayload(sub=user.username, exp=expire, scopes=user.scopes).dict()

        encoded_jwt = jwt.encode(
            payload, self.config.secret_key, algorithm=self.config.algorithm
        )
        return encoded_jwt

    def verify_token(self, token: str) -> Optional[TokenPayload]:
        """Verify and decode a JWT token."""
        try:
            payload = jwt.decode(
                token, self.config.secret_key, algorithms=[self.config.algorithm]
            )
            return TokenPayload(**payload)
        except jwt.PyJWTError:
            return None

    async def get_current_user(
        self, credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer())
    ) -> User:
        """Dependency to get current user from JWT token."""
        token_payload = self.verify_token(credentials.credentials)
        if token_payload is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )

        user = self.users_db.get(token_payload.sub)
        if user is None or user.disabled:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found or disabled",
            )

        return user

    def require_scope(self, scope: str):
        """Dependency factory to require specific scope."""

        def scope_dependency(current_user: User = Depends(get_current_user)):
            if scope not in current_user.scopes:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Requires scope: {scope}",
                )
            return current_user

        return scope_dependency


# Global authenticator instance
security_config = SecurityConfig()
authenticator = Authenticator(security_config)

# Skill: config_env_manager

The agent manages application configuration centrally and securely using environment variables and config files.

## Responsibility
Centralize configuration management, support multiple environments, and keep secrets secure.

## Rules
- Use environment variables for environment-specific settings
- Use config files for application defaults
- Never commit secrets to version control
- Support multiple environments (dev, staging, prod)
- Validate configuration on startup
- Use Pydantic Settings for type-safe config
- Document all configuration options
- Provide sensible defaults

## Behavior

### Step 1: Define Configuration Schema
- Create Pydantic Settings model
- Define all configuration parameters
- Set default values
- Add validation rules

### Step 2: Load from Multiple Sources
- Environment variables (highest priority)
- .env files (development)
- Config files (YAML, TOML, JSON)
- Defaults (lowest priority)

### Step 3: Validate Configuration
- Check required fields are present
- Validate types and constraints
- Fail fast on invalid configuration

### Step 4: Provide Access to Config
- Create singleton or dependency injection
- Make config immutable after loading
- Log configuration (excluding secrets)

## Example Usage

**Pydantic Settings Configuration:**
```python
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, PostgresDsn, validator
from typing import Optional

class Settings(BaseSettings):
    """Application configuration."""
    
    # Application settings
    app_name: str = "MyApp"
    debug: bool = False
    environment: str = Field(default="development", pattern="^(development|staging|production)$")
    
    # Server settings
    host: str = "0.0.0.0"
    port: int = Field(default=8000, ge=1, le=65535)
    
    # Database settings
    database_url: PostgresDsn
    database_pool_size: int = Field(default=10, ge=1, le=100)
    
    # Security settings
    secret_key: str = Field(..., min_length=32)
    api_key: Optional[str] = None
    allowed_hosts: list[str] = ["localhost", "127.0.0.1"]
    
    # External services
    redis_url: Optional[str] = None
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    
    # Feature flags
    enable_caching: bool = True
    enable_metrics: bool = False
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    @validator("secret_key")
    def validate_secret_key(cls, v):
        """Ensure secret key is strong."""
        if v == "changeme" or v == "secret":
            raise ValueError("Secret key must be changed from default")
        return v
    
    def is_production(self) -> bool:
        """Check if running in production."""
        return self.environment == "production"

# Create global settings instance
settings = Settings()
```

**.env File Example:**
```env
# Application
APP_NAME=MyApp
DEBUG=true
ENVIRONMENT=development

# Server
HOST=0.0.0.0
PORT=8000

# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/mydb
DATABASE_POOL_SIZE=20

# Security
SECRET_KEY=your-super-secret-key-min-32-chars-long
API_KEY=your-api-key-here

# External Services
REDIS_URL=redis://localhost:6379/0
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587

# Feature Flags
ENABLE_CACHING=true
ENABLE_METRICS=false
```

**Environment-Specific Config Files:**
```python
import yaml
from pathlib import Path
from typing import Any

class ConfigLoader:
    """Load configuration from YAML files."""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
    
    def load(self, environment: str) -> dict[str, Any]:
        """Load config for specific environment."""
        # Load base config
        base_config = self._load_file("base.yaml")
        
        # Load environment-specific config
        env_config = self._load_file(f"{environment}.yaml")
        
        # Merge configs (env overrides base)
        return {**base_config, **env_config}
    
    def _load_file(self, filename: str) -> dict[str, Any]:
        """Load YAML file."""
        filepath = self.config_dir / filename
        
        if not filepath.exists():
            return {}
        
        with open(filepath) as f:
            return yaml.safe_load(f) or {}

# Usage
loader = ConfigLoader()
config = loader.load(environment="production")
```

**Dependency Injection with FastAPI:**
```python
from fastapi import Depends, FastAPI
from functools import lru_cache

app = FastAPI()

@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()

@app.get("/info")
def get_info(settings: Settings = Depends(get_settings)):
    """Get application info."""
    return {
        "app_name": settings.app_name,
        "environment": settings.environment,
        "debug": settings.debug
    }
```

**Configuration with Validation:**
```python
from pydantic import BaseSettings, validator, Field
from typing import Optional
import logging

class DatabaseConfig(BaseSettings):
    """Database configuration."""
    host: str = "localhost"
    port: int = 5432
    username: str
    password: str
    database: str
    pool_size: int = Field(default=10, ge=1, le=100)
    
    @property
    def url(self) -> str:
        """Build database URL."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    class Config:
        env_prefix = "DB_"

class AppConfig(BaseSettings):
    """Main application configuration."""
    
    # Nested config
    database: DatabaseConfig = DatabaseConfig()
    
    # Logging
    log_level: str = Field(default="INFO", pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")
    
    @validator("log_level")
    def setup_logging(cls, v):
        """Configure logging based on level."""
        logging.basicConfig(
            level=getattr(logging, v),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        return v

# Usage
config = AppConfig()
print(f"Database URL: {config.database.url}")
```

**Config with Secrets from File:**
```python
from pathlib import Path
from pydantic import BaseSettings, SecretStr

class SecureSettings(BaseSettings):
    """Settings with secret handling."""
    
    api_key: SecretStr
    database_password: SecretStr
    
    class Config:
        secrets_dir = "/run/secrets"  # Docker secrets
    
    def get_api_key(self) -> str:
        """Get API key as string."""
        return self.api_key.get_secret_value()

# Secrets can be loaded from files in secrets_dir
# /run/secrets/api_key
# /run/secrets/database_password
```

**Configuration Testing:**
```python
import pytest
from pydantic import ValidationError

def test_settings_validation():
    """Test settings validation."""
    # Valid config
    settings = Settings(
        database_url="postgresql://user:pass@localhost/db",
        secret_key="a" * 32
    )
    assert settings.port == 8000
    
    # Invalid port
    with pytest.raises(ValidationError):
        Settings(
            database_url="postgresql://user:pass@localhost/db",
            secret_key="a" * 32,
            port=99999
        )
    
    # Invalid secret key
    with pytest.raises(ValidationError):
        Settings(
            database_url="postgresql://user:pass@localhost/db",
            secret_key="short"
        )
```

## Notes
- Use different .env files for different environments (.env.dev, .env.prod)
- Add .env to .gitignore
- Provide .env.example with dummy values
- Use environment variables in production, not .env files
- Consider using AWS Secrets Manager, HashiCorp Vault for production secrets
- Log configuration on startup (excluding sensitive values)

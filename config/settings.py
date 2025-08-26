# # Configuration management

# class Settings:
#     """Application settings from environment variables"""

# def load_environment_variables():
#     """Load and validate environment variables"""

# def get_tenant_config():
#     """Get tenant-specific configuration"""

# def get_database_config():
#     """Get MongoDB connection configuration"""

# def get_openai_config():
#     """Get OpenAI API configuration"""

import os
from dotenv import load_dotenv
from pydantic import BaseModel, Field
import structlog

load_dotenv()

logger = structlog.get_logger(__name__)


class TenantConfig(BaseModel):
    tenant_id: str = Field(default="default_tenant")


class DatabaseConfig(BaseModel):
    connection_string: str = Field(default="mongodb://localhost:27017/")
    database_name: str = Field(default="test_database")


class OpenAIConfig(BaseModel):
    api_key: str = Field(default="")
    model: str = Field(default="gpt-4")
    max_tokens: int = Field(default=4000)
    # monthly_budget_limit: int = Field(default=400)


class Settings:
    """Application settings from environment variables"""

    def __init__(self):
        self.tenant = self._load_tenant_config()
        self.database = self._load_database_config()
        self.openai = self._load_openai_config()
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.debug_mode = os.getenv("DEBUG_MODE", "False").lower() == "true"

    def _load_tenant_config(self) -> TenantConfig:
        tenant_id = os.getenv("TENANT_ID")
        if not tenant_id:
            logger.warning("TENANT_ID missing from env, using default")
        return TenantConfig(tenant_id=tenant_id or "demo_tenant_001")

    def _load_database_config(self) -> DatabaseConfig:
        conn = os.getenv("MONGODB_CONNECTION_STRING")
        db = os.getenv("MONGODB_DATABASE_NAME")
        if not conn or not db:
            logger.warning("MongoDB config incomplete, falling back to defaults")
        return DatabaseConfig(
            connection_string=conn or "mongodb://localhost:27017/",
            database_name=db or "fallback_db",
        )

    def _load_openai_config(self) -> OpenAIConfig:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.warning("OPENAI_API_KEY missing, OpenAI calls may fail")
        return OpenAIConfig(
            api_key=api_key or "",
            model=os.getenv("OPENAI_MODEL", "gpt-4"),
            max_tokens=int(os.getenv("MAX_TOKENS_PER_QUERY", "4000")),
            monthly_budget_limit=int(os.getenv("MONTHLY_BUDGET_LIMIT", "400")),
        )


# --- Public helper functions ---
def load_environment_variables() -> Settings:
    """Load and validate environment variables"""
    return Settings()


def get_tenant_config() -> dict:
    """Get tenant-specific configuration"""
    return load_environment_variables().tenant.model_dump()


def get_database_config() -> dict:
    """Get MongoDB connection configuration"""
    return load_environment_variables().database.model_dump()


def get_openai_config() -> dict:
    """Get OpenAI API configuration"""
    return load_environment_variables().openai.model_dump()

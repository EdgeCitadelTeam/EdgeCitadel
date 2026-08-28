"""Stable errors raised while inspecting plugin packages."""


class PluginError(RuntimeError):
    """Base class for plugin package failures."""


class PluginNotFoundError(PluginError):
    """Raised when a plugin package root cannot be found."""


class ManifestLoadError(PluginError):
    """Raised when a plugin manifest cannot be loaded safely."""


class ManifestValidationError(PluginError):
    """Raised when a plugin manifest does not match its schema."""


class CompatibilityError(PluginError):
    """Raised when a plugin is incompatible with the supervisor."""


class UnsafePackagePathError(PluginError):
    """Raised when a package path crosses a safety boundary."""


class SkillDiscoveryError(PluginError):
    """Raised when packaged skills cannot be discovered."""


class DuplicateSkillError(PluginError):
    """Raised when packaged skills have duplicate identifiers."""


class LockIntegrityError(PluginError):
    """Raised when package content does not match its lock file."""

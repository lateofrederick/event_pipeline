class Style:
    """Terminal styling helper."""

    # ANSI color codes
    COLORS = {
        "SUCCESS": "\033[92m",
        "WARNING": "\033[93m",
        "ERROR": "\033[91m",
        "NOTICE": "\033[94m",
        "RESET": "\033[0m",
        "BOLD": "\033[1m",
    }

    def SUCCESS(self, text: str) -> str:
        return f"{self.COLORS['SUCCESS']}{text}{self.COLORS['RESET']}"

    def WARNING(self, text: str) -> str:
        return f"{self.COLORS['WARNING']}{text}{self.COLORS['RESET']}"

    def ERROR(self, text: str) -> str:
        return f"{self.COLORS['ERROR']}{text}{self.COLORS['RESET']}"

    def NOTICE(self, text: str) -> str:
        return f"{self.COLORS['NOTICE']}{text}{self.COLORS['RESET']}"

    def BOLD(self, text: str) -> str:
        return f"{self.COLORS['BOLD']}{text}{self.COLORS['RESET']}"
